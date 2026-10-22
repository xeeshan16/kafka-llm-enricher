package io.github.xeeshan16.enricher.engine;

import io.github.xeeshan16.enricher.json.CanonicalJson;
import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.LedgerBusyException;
import io.github.xeeshan16.enricher.ledger.LedgerEntry;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps {@link Enricher} with a {@link ResultLedger} so that a record whose enrichment was already
 * paid for is answered from the ledger, and two workers never call the provider for the same key at
 * the same time.
 *
 * <p>The only window in which a duplicate paid call remains possible is a worker dying (or its
 * lease lapsing) after the provider answered but before {@link ResultLedger#complete} stored the
 * answer. Keep the lease longer than the provider timeout plus retries to keep that window small.
 */
public final class LedgeredEnricher {

  private static final Logger log = LoggerFactory.getLogger(LedgeredEnricher.class);

  /**
   * Lease and wait settings.
   *
   * @param leaseTtl how long a claim is held before another worker may take over
   * @param busyWait how long to wait in place for another worker's lease before giving up with
   *     {@link LedgerBusyException}; keep it well below the consumer's {@code max.poll.interval.ms}
   * @param pollInterval how often to re-check a busy key while waiting
   */
  public record Settings(Duration leaseTtl, Duration busyWait, Duration pollInterval) {
    public Settings {
      requirePositive(leaseTtl, "leaseTtl");
      Objects.requireNonNull(busyWait, "busyWait");
      if (busyWait.isNegative()) {
        throw new IllegalArgumentException("busyWait must not be negative");
      }
      requirePositive(pollInterval, "pollInterval");
    }

    public static Settings defaults() {
      return new Settings(Duration.ofMinutes(2), Duration.ofSeconds(2), Duration.ofMillis(200));
    }

    private static void requirePositive(Duration d, String name) {
      Objects.requireNonNull(d, name);
      if (d.isNegative() || d.isZero()) {
        throw new IllegalArgumentException(name + " must be positive");
      }
    }
  }

  /** Pause between busy checks; replaceable in tests. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
  }

  /**
   * The outcome for one record and where its enrichment came from.
   *
   * @param outcome enriched or failed
   * @param source {@link ResultSource#LEDGER} when no provider call was made for this delivery
   */
  public record Result(EnrichmentOutcome outcome, ResultSource source) {}

  private final Enricher enricher;
  private final ResultLedger ledger;
  private final Settings settings;
  private final Clock clock;
  private final Sleeper sleeper;

  public LedgeredEnricher(Enricher enricher, ResultLedger ledger, Settings settings) {
    this(enricher, ledger, settings, Clock.systemUTC(), d -> Thread.sleep(d.toMillis()));
  }

  public LedgeredEnricher(
      Enricher enricher, ResultLedger ledger, Settings settings, Clock clock, Sleeper sleeper) {
    this.enricher = Objects.requireNonNull(enricher, "enricher");
    this.ledger = Objects.requireNonNull(ledger, "ledger");
    this.settings = Objects.requireNonNull(settings, "settings");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
  }

  public Settings settings() {
    return settings;
  }

  /** The ledger key of a prepared record. */
  public static LedgerKey keyOf(PreparedRecord prepared) {
    PipelineDefinition pipeline = prepared.pipeline();
    return new LedgerKey(
        pipeline.name(), pipeline.promptVersion(), pipeline.model(), prepared.inputHash());
  }

  /**
   * Enriches {@code record}, from the ledger when possible.
   *
   * @throws LedgerBusyException when another worker holds the key's lease for longer than the
   *     configured busy wait; the caller should retry the record later
   * @throws io.github.xeeshan16.enricher.llm.LlmProviderException when the provider call failed
   */
  public Result process(PipelineDefinition pipeline, InputRecord record) {
    PreparedRecord prepared = enricher.prepare(pipeline, record);
    LedgerKey key = keyOf(prepared);
    Instant deadline = clock.instant().plus(settings.busyWait());
    while (true) {
      Claim claim = ledger.claim(key, settings.leaseTtl());
      switch (claim) {
        case Claim.Hit hit -> {
          return new Result(fromLedger(prepared, hit.entry()), ResultSource.LEDGER);
        }
        case Claim.Acquired acquired -> {
          return callAndComplete(prepared, key, acquired.token());
        }
        case Claim.Busy busy -> {
          if (!clock.instant().isBefore(deadline)) {
            throw new LedgerBusyException(key, busy.holder(), busy.leaseExpiresAt());
          }
          pause();
        }
      }
    }
  }

  private Result callAndComplete(PreparedRecord prepared, LedgerKey key, String token) {
    LlmResponse response;
    EnrichmentOutcome outcome;
    try {
      response = enricher.provider().complete(prepared.request());
      outcome = enricher.interpret(prepared, response);
    } catch (RuntimeException e) {
      releaseQuietly(key, token);
      throw e;
    }
    switch (outcome) {
      case EnrichmentOutcome.Failed failed -> {
        releaseQuietly(key, token);
        return new Result(failed, ResultSource.PROVIDER);
      }
      case EnrichmentOutcome.Enriched enriched -> {
        LedgerEntry ours =
            new LedgerEntry(
                CanonicalJson.write(enriched.envelope().get("enrichment")),
                response.model(),
                response.usage());
        LedgerEntry winner = ledger.complete(key, ours);
        if (winner.equals(ours)) {
          return new Result(enriched, ResultSource.PROVIDER);
        }
        log.info("ledger key {} was completed by another worker first; publishing its result", key);
        return new Result(fromLedger(prepared, winner), ResultSource.LEDGER);
      }
    }
  }

  private EnrichmentOutcome.Enriched fromLedger(PreparedRecord prepared, LedgerEntry entry) {
    return enricher.fromStored(
        prepared, entry.enrichmentJson(), entry.responseModel(), entry.usage());
  }

  private void releaseQuietly(LedgerKey key, String token) {
    try {
      ledger.release(key, token);
    } catch (RuntimeException e) {
      log.warn("cannot release ledger lease for {}; it lapses on its own: {}", key, e.toString());
    }
  }

  private void pause() {
    try {
      sleeper.sleep(settings.pollInterval());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for a ledger lease", e);
    }
  }
}
