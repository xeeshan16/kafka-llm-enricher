package io.github.xeeshan16.enricher.ledger.postgres;

import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.LedgerEntry;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * {@link ResultLedger} on PostgreSQL, shared by every instance of a pipeline.
 *
 * <p>Every operation is one auto-committed statement, so it is atomic on its own and independent of
 * any Kafka or application transaction: a stored result survives even when the Kafka transaction
 * that was going to publish it aborts, which is exactly what makes the retry free. Lease times use
 * the database clock ({@code clock_timestamp()}), so instances with skewed clocks agree on when a
 * lease lapses.
 *
 * <p>Run {@link LedgerSchema#migrate} (or the starter's {@code enricher.ledger.initialize-schema})
 * once to create the table.
 */
public final class PostgresResultLedger implements ResultLedger {

  private static final String CLAIM =
      """
      INSERT INTO enricher_result_ledger AS l
          (pipeline, prompt_version, model, input_hash, status,
           lease_token, lease_owner, lease_expires_at, attempts)
      VALUES (?, ?, ?, ?, 'IN_FLIGHT', ?, ?, clock_timestamp() + make_interval(secs => ?), 1)
      ON CONFLICT (pipeline, prompt_version, model, input_hash) DO UPDATE
          SET lease_token = EXCLUDED.lease_token,
              lease_owner = EXCLUDED.lease_owner,
              lease_expires_at = EXCLUDED.lease_expires_at,
              attempts = l.attempts + 1
          WHERE l.status = 'IN_FLIGHT' AND l.lease_expires_at <= clock_timestamp()
      RETURNING l.lease_token
      """;

  private static final String HIT =
      """
      UPDATE enricher_result_ledger
         SET hits = hits + 1, last_hit_at = clock_timestamp()
       WHERE pipeline = ? AND prompt_version = ? AND model = ? AND input_hash = ?
         AND status = 'COMPLETED'
      RETURNING result_json, response_model, input_tokens, output_tokens
      """;

  private static final String LEASE =
      """
      SELECT lease_owner, lease_expires_at FROM enricher_result_ledger
       WHERE pipeline = ? AND prompt_version = ? AND model = ? AND input_hash = ?
         AND status = 'IN_FLIGHT'
      """;

  private static final String COMPLETE =
      """
      INSERT INTO enricher_result_ledger AS l
          (pipeline, prompt_version, model, input_hash, status,
           result_json, response_model, input_tokens, output_tokens, completed_at)
      VALUES (?, ?, ?, ?, 'COMPLETED', ?, ?, ?, ?, clock_timestamp())
      ON CONFLICT (pipeline, prompt_version, model, input_hash) DO UPDATE
          SET status = 'COMPLETED',
              result_json = EXCLUDED.result_json,
              response_model = EXCLUDED.response_model,
              input_tokens = EXCLUDED.input_tokens,
              output_tokens = EXCLUDED.output_tokens,
              completed_at = EXCLUDED.completed_at,
              lease_token = NULL,
              lease_owner = NULL,
              lease_expires_at = NULL
          WHERE l.status = 'IN_FLIGHT'
      RETURNING l.result_json
      """;

  private static final String STORED =
      """
      SELECT result_json, response_model, input_tokens, output_tokens FROM enricher_result_ledger
       WHERE pipeline = ? AND prompt_version = ? AND model = ? AND input_hash = ?
         AND status = 'COMPLETED'
      """;

  private static final String RELEASE =
      """
      DELETE FROM enricher_result_ledger
       WHERE pipeline = ? AND prompt_version = ? AND model = ? AND input_hash = ?
         AND status = 'IN_FLIGHT' AND lease_token = ?
      """;

  private static final String PURGE =
      """
      DELETE FROM enricher_result_ledger
       WHERE status = 'COMPLETED' AND completed_at < ?
      """;

  /** Claim, hit and lease lookups race with other workers; retry the sequence a few times. */
  private static final int CLAIM_ROUNDS = 5;

  private final DataSource dataSource;
  private final String owner;

  /**
   * @param dataSource connections to the database holding {@code enricher_result_ledger}
   * @param owner name of this instance, stored with its leases for diagnostics (for example the pod
   *     name)
   */
  public PostgresResultLedger(DataSource dataSource, String owner) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.owner = Objects.requireNonNull(owner, "owner");
  }

  @Override
  public Claim claim(LedgerKey key, Duration leaseTtl) {
    double seconds = leaseTtl.toMillis() / 1000.0;
    try (Connection c = connection()) {
      for (int round = 0; round < CLAIM_ROUNDS; round++) {
        String token = UUID.randomUUID().toString();
        try (PreparedStatement ps = c.prepareStatement(CLAIM)) {
          int i = bindKey(ps, key);
          ps.setString(i++, token);
          ps.setString(i++, owner);
          ps.setDouble(i, seconds);
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
              return new Claim.Acquired(rs.getString(1));
            }
          }
        }
        LedgerEntry stored = query(c, HIT, key);
        if (stored != null) {
          return new Claim.Hit(stored);
        }
        try (PreparedStatement ps = c.prepareStatement(LEASE)) {
          bindKey(ps, key);
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
              Timestamp expires = rs.getTimestamp(2);
              return new Claim.Busy(rs.getString(1), expires == null ? null : expires.toInstant());
            }
          }
        }
        // The row changed between the statements (released or purged); try again.
      }
      throw new IllegalStateException("ledger key " + key + " kept changing while being claimed");
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot claim ledger key " + key, e);
    }
  }

  @Override
  public LedgerEntry complete(LedgerKey key, LedgerEntry entry) {
    Objects.requireNonNull(entry, "entry");
    try (Connection c = connection()) {
      try (PreparedStatement ps = c.prepareStatement(COMPLETE)) {
        int i = bindKey(ps, key);
        ps.setString(i++, entry.enrichmentJson());
        ps.setString(i++, entry.responseModel());
        ps.setLong(i++, entry.usage().inputTokens());
        ps.setLong(i, entry.usage().outputTokens());
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return entry;
          }
        }
      }
      LedgerEntry stored = query(c, STORED, key);
      if (stored == null) {
        throw new IllegalStateException("ledger key " + key + " is neither stored nor writable");
      }
      return stored;
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot complete ledger key " + key, e);
    }
  }

  @Override
  public void release(LedgerKey key, String token) {
    try (Connection c = connection();
        PreparedStatement ps = c.prepareStatement(RELEASE)) {
      int i = bindKey(ps, key);
      ps.setString(i, token);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot release ledger key " + key, e);
    }
  }

  @Override
  public int purgeCompletedBefore(Instant completedBefore) {
    try (Connection c = connection();
        PreparedStatement ps = c.prepareStatement(PURGE)) {
      ps.setTimestamp(1, Timestamp.from(completedBefore));
      return ps.executeUpdate();
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot purge the result ledger", e);
    }
  }

  private Connection connection() throws SQLException {
    Connection c = dataSource.getConnection();
    if (!c.getAutoCommit()) {
      c.setAutoCommit(true);
    }
    return c;
  }

  private static LedgerEntry query(Connection c, String sql, LedgerKey key) throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      bindKey(ps, key);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        return new LedgerEntry(
            rs.getString(1), rs.getString(2), new TokenUsage(rs.getLong(3), rs.getLong(4)));
      }
    }
  }

  /** Binds the four key columns and returns the next parameter index. */
  private static int bindKey(PreparedStatement ps, LedgerKey key) throws SQLException {
    ps.setString(1, key.pipeline());
    ps.setString(2, key.promptVersion());
    ps.setString(3, key.model());
    ps.setString(4, key.inputHash());
    return 5;
  }
}
