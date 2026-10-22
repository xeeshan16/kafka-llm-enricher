package io.github.xeeshan16.enricher.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.MutableClock;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class InMemoryResultLedgerTest {

  private static final LedgerKey KEY = new LedgerKey("triage", "v1", "fake-1", "sha256:aa");
  private static final LedgerEntry ENTRY =
      new LedgerEntry("{\"label\":\"refund\"}", "fake-1", new TokenUsage(10, 3));
  private static final Duration TTL = Duration.ofSeconds(30);

  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-22T10:00:00Z"));
  private final InMemoryResultLedger ledger = new InMemoryResultLedger(clock, "test");

  @Test
  void firstClaimAcquiresSecondIsBusyUntilCompleted() {
    assertThat(ledger.claim(KEY, TTL)).isInstanceOf(Claim.Acquired.class);
    assertThat(ledger.claim(KEY, TTL))
        .isInstanceOfSatisfying(
            Claim.Busy.class,
            busy -> assertThat(busy.leaseExpiresAt()).isEqualTo(clock.instant().plus(TTL)));

    assertThat(ledger.complete(KEY, ENTRY)).isEqualTo(ENTRY);

    assertThat(ledger.claim(KEY, TTL)).isEqualTo(new Claim.Hit(ENTRY));
    assertThat(ledger.claim(KEY, TTL)).isEqualTo(new Claim.Hit(ENTRY));
    assertThat(ledger.hits(KEY)).isEqualTo(2);
  }

  @Test
  void expiredLeaseCanBeTakenOver() {
    ledger.claim(KEY, TTL);
    clock.advance(TTL);

    assertThat(ledger.claim(KEY, TTL)).isInstanceOf(Claim.Acquired.class);
  }

  @Test
  void firstCompletedResultWins() {
    LedgerEntry late = new LedgerEntry("{\"label\":\"other\"}", "fake-1", new TokenUsage(9, 2));
    ledger.claim(KEY, TTL);
    clock.advance(TTL.plusSeconds(1));
    ledger.claim(KEY, TTL);

    assertThat(ledger.complete(KEY, ENTRY)).isEqualTo(ENTRY);
    assertThat(ledger.complete(KEY, late)).isEqualTo(ENTRY);
  }

  @Test
  void releaseOnlyDropsTheMatchingLease() {
    Claim.Acquired first = (Claim.Acquired) ledger.claim(KEY, TTL);
    ledger.release(KEY, "someone-else");
    assertThat(ledger.claim(KEY, TTL)).isInstanceOf(Claim.Busy.class);

    ledger.release(KEY, first.token());
    assertThat(ledger.claim(KEY, TTL)).isInstanceOf(Claim.Acquired.class);
  }

  @Test
  void releaseNeverDropsACompletedResult() {
    Claim.Acquired acquired = (Claim.Acquired) ledger.claim(KEY, TTL);
    ledger.complete(KEY, ENTRY);
    ledger.release(KEY, acquired.token());

    assertThat(ledger.claim(KEY, TTL)).isEqualTo(new Claim.Hit(ENTRY));
  }

  @Test
  void purgeRemovesOldCompletedResultsOnly() {
    LedgerKey other = new LedgerKey("triage", "v1", "fake-1", "sha256:bb");
    ledger.claim(KEY, TTL);
    ledger.complete(KEY, ENTRY);
    ledger.claim(other, TTL);
    clock.advance(Duration.ofDays(8));

    assertThat(ledger.purgeCompletedBefore(clock.instant().minus(Duration.ofDays(7)))).isOne();
    assertThat(ledger.claim(KEY, TTL)).isInstanceOf(Claim.Acquired.class);
  }

  @Test
  void concurrentClaimsGrantExactlyOneLease() throws Exception {
    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Callable<Claim>> tasks = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        tasks.add(() -> ledger.claim(KEY, TTL));
      }
      long acquired = 0;
      for (Future<Claim> f : pool.invokeAll(tasks)) {
        if (f.get() instanceof Claim.Acquired) {
          acquired++;
        }
      }
      assertThat(acquired).isOne();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void noopLedgerAlwaysGrants() {
    NoopResultLedger noop = new NoopResultLedger();
    assertThat(noop.claim(KEY, TTL)).isInstanceOf(Claim.Acquired.class);
    assertThat(noop.claim(KEY, TTL)).isInstanceOf(Claim.Acquired.class);
    assertThat(noop.complete(KEY, ENTRY)).isEqualTo(ENTRY);
    assertThat(noop.purgeCompletedBefore(Instant.MAX)).isZero();
  }
}
