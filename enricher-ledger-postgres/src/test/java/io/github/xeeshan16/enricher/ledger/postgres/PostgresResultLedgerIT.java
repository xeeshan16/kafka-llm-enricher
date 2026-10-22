package io.github.xeeshan16.enricher.ledger.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.LedgerEntry;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresResultLedgerIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  private static final Duration TTL = Duration.ofSeconds(30);
  private static final LedgerEntry ENTRY =
      new LedgerEntry("{\"label\":\"refund\"}", "fake-1", new TokenUsage(120, 40));

  private static PGSimpleDataSource dataSource;

  private final PostgresResultLedger ledger = new PostgresResultLedger(dataSource, "it-worker");

  @BeforeAll
  static void migrate() throws SQLException {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    // An application table already in the schema: the ledger must still create its own table.
    execute("CREATE TABLE app_orders (id bigint PRIMARY KEY)");
    LedgerSchema.migrate(dataSource);
  }

  private static LedgerKey newKey() {
    return new LedgerKey("triage", "v1", "fake-1", "sha256:" + UUID.randomUUID());
  }

  @Test
  void migrationIsIdempotentAndCoexistsWithApplicationTables() throws SQLException {
    assertThat(LedgerSchema.migrate(dataSource).migrationsExecuted).isZero();
    assertThat(count("SELECT count(*) FROM app_orders")).isZero();
    assertThat(
            count(
                "SELECT count(*) FROM "
                    + LedgerSchema.HISTORY_TABLE
                    + " WHERE version = '1' AND success"))
        .isOne();
  }

  @Test
  void claimCompleteThenHit() throws SQLException {
    LedgerKey key = newKey();

    assertThat(ledger.claim(key, TTL)).isInstanceOf(Claim.Acquired.class);
    assertThat(ledger.claim(key, TTL))
        .isInstanceOfSatisfying(
            Claim.Busy.class,
            busy -> {
              assertThat(busy.holder()).isEqualTo("it-worker");
              assertThat(busy.leaseExpiresAt()).isAfter(Instant.now().plusSeconds(20));
            });
    assertThat(ledger.complete(key, ENTRY)).isEqualTo(ENTRY);
    assertThat(ledger.claim(key, TTL)).isEqualTo(new Claim.Hit(ENTRY));
    assertThat(ledger.claim(key, TTL)).isEqualTo(new Claim.Hit(ENTRY));

    assertThat(
            count("SELECT hits FROM enricher_result_ledger WHERE input_hash = '" + hash(key) + "'"))
        .isEqualTo(2);
  }

  @Test
  void storedJsonIsReturnedByteForByte() {
    LedgerKey key = newKey();
    LedgerEntry spaced =
        new LedgerEntry("{\"b\":1,\"a\":[1.50,\"x\"]}", "model-x", new TokenUsage(1, 2));
    ledger.claim(key, TTL);
    ledger.complete(key, spaced);

    assertThat(ledger.claim(key, TTL)).isEqualTo(new Claim.Hit(spaced));
  }

  @Test
  void lapsedLeaseIsTakenOverAndCountsAttempts() throws Exception {
    LedgerKey key = newKey();
    ledger.claim(key, Duration.ofMillis(200));
    Thread.sleep(400);

    assertThat(ledger.claim(key, TTL)).isInstanceOf(Claim.Acquired.class);
    assertThat(
            count(
                "SELECT attempts FROM enricher_result_ledger WHERE input_hash = '"
                    + hash(key)
                    + "'"))
        .isEqualTo(2);
  }

  @Test
  void lateCompletionKeepsTheFirstResult() {
    LedgerKey key = newKey();
    LedgerEntry late = new LedgerEntry("{\"label\":\"other\"}", "fake-1", new TokenUsage(9, 9));
    ledger.claim(key, TTL);

    assertThat(ledger.complete(key, ENTRY)).isEqualTo(ENTRY);
    assertThat(ledger.complete(key, late)).isEqualTo(ENTRY);
  }

  @Test
  void completionWithoutALeaseRowStillStores() {
    LedgerKey key = newKey();
    Claim.Acquired acquired = (Claim.Acquired) ledger.claim(key, TTL);
    ledger.release(key, acquired.token());

    assertThat(ledger.complete(key, ENTRY)).isEqualTo(ENTRY);
    assertThat(ledger.claim(key, TTL)).isEqualTo(new Claim.Hit(ENTRY));
  }

  @Test
  void releaseDropsOnlyTheMatchingLease() {
    LedgerKey key = newKey();
    Claim.Acquired acquired = (Claim.Acquired) ledger.claim(key, TTL);

    ledger.release(key, "not-the-token");
    assertThat(ledger.claim(key, TTL)).isInstanceOf(Claim.Busy.class);

    ledger.release(key, acquired.token());
    assertThat(ledger.claim(key, TTL)).isInstanceOf(Claim.Acquired.class);
  }

  @Test
  void concurrentWorkersOnSeparateConnectionsGetExactlyOneLease() throws Exception {
    int workers = 24;
    LedgerKey key = newKey();
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Callable<Claim>> tasks = new ArrayList<>();
      for (int i = 0; i < workers; i++) {
        PostgresResultLedger worker = new PostgresResultLedger(dataSource, "worker-" + i);
        tasks.add(
            () -> {
              start.await();
              return worker.claim(key, TTL);
            });
      }
      List<Future<Claim>> futures = new ArrayList<>();
      for (Callable<Claim> task : tasks) {
        futures.add(pool.submit(task));
      }
      start.countDown();
      int acquired = 0;
      int busy = 0;
      for (Future<Claim> f : futures) {
        switch (f.get()) {
          case Claim.Acquired a -> acquired++;
          case Claim.Busy b -> busy++;
          case Claim.Hit h -> throw new AssertionError("nothing was completed yet");
        }
      }
      assertThat(acquired).isOne();
      assertThat(busy).isEqualTo(workers - 1);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void purgeDeletesOldCompletedRowsButKeepsLeases() throws SQLException {
    LedgerKey done = newKey();
    LedgerKey leased = newKey();
    ledger.claim(done, TTL);
    ledger.complete(done, ENTRY);
    ledger.claim(leased, TTL);
    execute(
        "UPDATE enricher_result_ledger SET completed_at = now() - interval '30 days'"
            + " WHERE input_hash = '"
            + hash(done)
            + "'");

    assertThat(ledger.purgeCompletedBefore(Instant.now().minus(Duration.ofDays(7))))
        .isGreaterThanOrEqualTo(1);
    assertThat(ledger.claim(done, TTL)).isInstanceOf(Claim.Acquired.class);
    assertThat(ledger.claim(leased, TTL)).isInstanceOf(Claim.Busy.class);
  }

  private static String hash(LedgerKey key) {
    return key.inputHash();
  }

  private static long count(String sql) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static void execute(String sql) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement s = c.createStatement()) {
      s.execute(sql);
    }
  }
}
