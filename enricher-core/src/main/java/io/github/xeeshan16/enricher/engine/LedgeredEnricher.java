package io.github.xeeshan16.enricher.engine;

import io.github.xeeshan16.enricher.cost.BudgetDecision;
import io.github.xeeshan16.enricher.cost.BudgetGuard;
import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import io.github.xeeshan16.enricher.cost.UsageMeter;
import io.github.xeeshan16.enricher.json.CanonicalJson;
import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.LedgerBusyException;
import io.github.xeeshan16.enricher.ledger.LedgerEntry;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.TokenEstimator;
import io.github.xeeshan16.enricher.llm.TokenUsage;
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

  /** Skip reason for a record whose tenant is out of budget and whose action is skip. */
  public static final String OVER_BUDGET = "over-budget";

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
   * @param outcome enriched, failed, skipped or deferred
   * @param source {@link ResultSource#LEDGER} when no provider call was made for this delivery,
   *     {@link ResultSource#NONE} when the record was skipped or deferred
   * @param tenant the tenant the record's spend is charged to
   */
  public record Result(EnrichmentOutcome outcome, ResultSource source, String tenant) {
    public Result(EnrichmentOutcome outcome, ResultSource source) {
      this(outcome, source, TenantResolver.DEFAULT_TENANT);
    }
  }

  /** One pass of claim-and-call for one model: done, or stopped by the budget. */
  private sealed interface Attempt {
    record Done(Result result) implements Attempt {}

    record OverBudget(BudgetDecision.Denied denial) implements Attempt {}
  }

  private final Enricher enricher;
  private final ResultLedger ledger;
  private final Settings settings;
  private final Clock clock;
  private final Sleeper sleeper;
  private final UsageMeter meter;

  public LedgeredEnricher(Enricher enricher, ResultLedger ledger, Settings settings) {
    this(enricher, ledger, settings, UsageMeter.NOOP);
  }

  public LedgeredEnricher(
      Enricher enricher, ResultLedger ledger, Settings settings, UsageMeter meter) {
    this(enricher, ledger, settings, Clock.systemUTC(), d -> Thread.sleep(d.toMillis()), meter);
  }

  public LedgeredEnricher(
      Enricher enricher, ResultLedger ledger, Settings settings, Clock clock, Sleeper sleeper) {
    this(enricher, ledger, settings, clock, sleeper, UsageMeter.NOOP);
  }

  public LedgeredEnricher(
      Enricher enricher,
      ResultLedger ledger,
      Settings settings,
      Clock clock,
      Sleeper sleeper,
      UsageMeter meter) {
    this.enricher = Objects.requireNonNull(enricher, "enricher");
    this.ledger = Objects.requireNonNull(ledger, "ledger");
    this.settings = Objects.requireNonNull(settings, "settings");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    this.meter = Objects.requireNonNull(meter, "meter");
  }

  public Settings settings() {
    return settings;
  }

  public UsageMeter meter() {
    return meter;
  }

  /** The ledger key of a prepared record. */
  public static LedgerKey keyOf(PreparedRecord prepared) {
    PipelineDefinition pipeline = prepared.pipeline();
    return new LedgerKey(
        pipeline.name(), pipeline.promptVersion(), pipeline.model(), prepared.inputHash());
  }

  /** Enriches {@code record} with no skip rules and no budgets. */
  public Result process(PipelineDefinition pipeline, InputRecord record) {
    return process(pipeline, record, CostPolicy.none());
  }

  /**
   * Enriches {@code record}, from the ledger when possible, applying the pipeline's cost controls in
   * this order:
   *
   * <ol>
   *   <li>skip rules: a matching record is answered with a skipped envelope and never sent;
   *   <li>result ledger: a stored answer is reused for free, whatever the budget says;
   *   <li>budget: only a call that is really going to be paid for reserves budget. When it does not
   *       fit, the record is downgraded to the fallback model (which gets its own ledger lookup),
   *       skipped, or deferred until the budget resets, as configured.
   * </ol>
   *
   * @throws LedgerBusyException when another worker holds the key's lease for longer than the
   *     configured busy wait; the caller should retry the record later
   * @throws io.github.xeeshan16.enricher.llm.LlmProviderException when the provider call failed
   */
  public Result process(PipelineDefinition pipeline, InputRecord record, CostPolicy policy) {
    PreparedRecord prepared = enricher.prepare(pipeline, record);
    String tenant = policy.tenants().tenantOf(prepared);
    for (SkipRule rule : policy.skipRules()) {
      if (matches(rule, prepared)) {
        return skip(prepared, tenant, "rule:" + rule.name());
      }
    }
    Attempt attempt = attempt(prepared, tenant, policy.budgets());
    OverBudgetAction action = policy.onExceeded();
    if (attempt instanceof Attempt.OverBudget
        && action == OverBudgetAction.DOWNGRADE
        && !policy.fallbackModel().equals(pipeline.model())) {
      meter.downgraded(pipeline.name(), tenant, pipeline.model(), policy.fallbackModel());
      log.debug(
          "pipeline={} tenant={} over budget on {}; trying {}",
          pipeline.name(),
          tenant,
          pipeline.model(),
          policy.fallbackModel());
      PreparedRecord cheaper =
          enricher.prepare(pipeline.withModel(policy.fallbackModel()), record);
      attempt = attempt(cheaper, tenant, policy.budgets());
      action = policy.afterDowngrade();
    } else if (action == OverBudgetAction.DOWNGRADE) {
      action = policy.afterDowngrade();
    }
    return switch (attempt) {
      case Attempt.Done done -> done.result();
      case Attempt.OverBudget over -> overBudget(prepared, tenant, over.denial(), action);
    };
  }

  private boolean matches(SkipRule rule, PreparedRecord prepared) {
    try {
      return rule.condition().test(prepared);
    } catch (RuntimeException e) {
      log.warn(
          "pipeline={} skip rule '{}' failed on {}-{}@{}; enriching the record: {}",
          prepared.pipeline().name(),
          rule.name(),
          prepared.record().topic(),
          prepared.record().partition(),
          prepared.record().offset(),
          e.toString());
      return false;
    }
  }

  private Result skip(PreparedRecord prepared, String tenant, String reason) {
    meter.skipped(prepared.pipeline().name(), tenant, reason);
    return new Result(
        new EnrichmentOutcome.Skipped(
            prepared.inputHash(), reason, enricher.skippedEnvelope(prepared, reason)),
        ResultSource.NONE,
        tenant);
  }

  private Result overBudget(
      PreparedRecord prepared,
      String tenant,
      BudgetDecision.Denied denial,
      OverBudgetAction action) {
    if (action == OverBudgetAction.SKIP) {
      return skip(prepared, tenant, OVER_BUDGET);
    }
    meter.deferred(prepared.pipeline().name(), tenant);
    return new Result(
        new EnrichmentOutcome.Deferred(
            prepared.inputHash(), tenant, denial.resetsAt(), denial.detail()),
        ResultSource.NONE,
        tenant);
  }

  private Attempt attempt(PreparedRecord prepared, String tenant, BudgetGuard budgets) {
    LedgerKey key = keyOf(prepared);
    Instant deadline = clock.instant().plus(settings.busyWait());
    while (true) {
      Claim claim = ledger.claim(key, settings.leaseTtl());
      switch (claim) {
        case Claim.Hit hit -> {
          LedgerEntry entry = hit.entry();
          meter.ledgerHit(prepared.pipeline().name(), tenant, key.model(), entry.usage());
          return new Attempt.Done(
              new Result(fromLedger(prepared, entry), ResultSource.LEDGER, tenant));
        }
        case Claim.Acquired acquired -> {
          BudgetDecision.Granted grant = null;
          if (budgets != null) {
            BudgetDecision decision =
                budgets.reserve(
                    prepared.pipeline().name(),
                    tenant,
                    key.model(),
                    TokenEstimator.estimate(prepared.request()));
            if (decision instanceof BudgetDecision.Denied denied) {
              releaseQuietly(key, acquired.token());
              return new Attempt.OverBudget(denied);
            }
            grant = (BudgetDecision.Granted) decision;
          }
          return new Attempt.Done(
              callAndComplete(prepared, key, acquired.token(), tenant, budgets, grant));
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

  private Result callAndComplete(
      PreparedRecord prepared,
      LedgerKey key,
      String token,
      String tenant,
      BudgetGuard budgets,
      BudgetDecision.Granted grant) {
    LlmResponse response;
    try {
      response = enricher.provider().complete(prepared.request());
    } catch (RuntimeException e) {
      if (grant != null) {
        refundQuietly(budgets, grant);
      }
      releaseQuietly(key, token);
      throw e;
    }
    // Paid for from here on, whatever the answer turns out to be.
    meter.providerCall(prepared.pipeline().name(), tenant, key.model(), response.usage());
    if (grant != null) {
      settleQuietly(budgets, grant, response.usage());
    }
    EnrichmentOutcome outcome;
    try {
      outcome = enricher.interpret(prepared, response);
    } catch (RuntimeException e) {
      releaseQuietly(key, token);
      throw e;
    }
    switch (outcome) {
      case EnrichmentOutcome.Enriched enriched -> {
        LedgerEntry ours =
            new LedgerEntry(
                CanonicalJson.write(enriched.envelope().get("enrichment")),
                response.model(),
                response.usage());
        LedgerEntry winner = ledger.complete(key, ours);
        if (winner.equals(ours)) {
          return new Result(enriched, ResultSource.PROVIDER, tenant);
        }
        log.info("ledger key {} was completed by another worker first; publishing its result", key);
        return new Result(fromLedger(prepared, winner), ResultSource.LEDGER, tenant);
      }
      default -> {
        releaseQuietly(key, token);
        return new Result(outcome, ResultSource.PROVIDER, tenant);
      }
    }
  }

  private void settleQuietly(BudgetGuard budgets, BudgetDecision.Granted grant, TokenUsage usage) {
    try {
      budgets.settle(grant, usage);
    } catch (RuntimeException e) {
      log.warn("cannot settle budget for {}; the estimate stays charged: {}", grant.key(), e.toString());
    }
  }

  private void refundQuietly(BudgetGuard budgets, BudgetDecision.Granted grant) {
    try {
      budgets.refund(grant);
    } catch (RuntimeException e) {
      log.warn("cannot refund budget for {}; the estimate stays charged: {}", grant.key(), e.toString());
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
