package io.github.xeeshan16.enricher.ledger.postgres;

import io.github.xeeshan16.enricher.cost.BudgetKey;
import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetStore;
import io.github.xeeshan16.enricher.cost.BudgetUsage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * {@link BudgetStore} on PostgreSQL, so every instance of a pipeline charges the same counters and
 * a tenant's cap holds across replicas.
 *
 * <p>{@link #tryCharge} is one {@code INSERT .. ON CONFLICT DO UPDATE .. WHERE} statement: the row
 * lock taken by the upsert serialises concurrent charges of one tenant, and the {@code WHERE}
 * clause sees the latest committed counters, so a charge that would cross the cap updates nothing.
 * The first charge of a window inserts the row; that case is checked against the cap before the
 * statement runs.
 *
 * <p>Uses the table created by {@link LedgerSchema#migrate} ({@code V2__tenant_budget_usage.sql}).
 */
public final class PostgresBudgetStore implements BudgetStore {

  private static final String TRY_CHARGE =
      """
      INSERT INTO enricher_budget_usage AS u (pipeline, tenant, period_start, tokens, cost_micros, calls)
      VALUES (?, ?, ?, ?, ?, 1)
      ON CONFLICT (pipeline, tenant, period_start) DO UPDATE
          SET tokens = u.tokens + EXCLUDED.tokens,
              cost_micros = u.cost_micros + EXCLUDED.cost_micros,
              calls = u.calls + 1,
              updated_at = clock_timestamp()
          WHERE (? < 0 OR u.tokens + EXCLUDED.tokens <= ?)
            AND (? < 0 OR u.cost_micros + EXCLUDED.cost_micros <= ?)
      """;

  private static final String ADJUST =
      """
      INSERT INTO enricher_budget_usage AS u (pipeline, tenant, period_start, tokens, cost_micros, calls)
      VALUES (?, ?, ?, GREATEST(0, ?), GREATEST(0, ?), GREATEST(0, ?))
      ON CONFLICT (pipeline, tenant, period_start) DO UPDATE
          SET tokens = GREATEST(0, u.tokens + ?),
              cost_micros = GREATEST(0, u.cost_micros + ?),
              calls = GREATEST(0, u.calls + ?),
              updated_at = clock_timestamp()
      """;

  private static final String USAGE =
      """
      SELECT tokens, cost_micros, calls FROM enricher_budget_usage
       WHERE pipeline = ? AND tenant = ? AND period_start = ?
      """;

  private static final String PURGE = "DELETE FROM enricher_budget_usage WHERE period_start < ?";

  private final DataSource dataSource;

  public PostgresBudgetStore(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  @Override
  public boolean tryCharge(BudgetKey key, long tokens, long costMicros, BudgetLimit limit) {
    if (!limit.allows(tokens, costMicros)) {
      return false; // does not fit even into an empty window
    }
    try (Connection c = connection();
        PreparedStatement ps = c.prepareStatement(TRY_CHARGE)) {
      int i = bindKey(ps, key);
      ps.setLong(i++, tokens);
      ps.setLong(i++, costMicros);
      ps.setLong(i++, limit.maxTokens());
      ps.setLong(i++, limit.maxTokens());
      ps.setLong(i++, limit.maxCostMicros());
      ps.setLong(i, limit.maxCostMicros());
      return ps.executeUpdate() == 1;
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot charge budget " + key, e);
    }
  }

  @Override
  public void adjust(BudgetKey key, long tokens, long costMicros, long calls) {
    try (Connection c = connection();
        PreparedStatement ps = c.prepareStatement(ADJUST)) {
      int i = bindKey(ps, key);
      ps.setLong(i++, tokens);
      ps.setLong(i++, costMicros);
      ps.setLong(i++, calls);
      ps.setLong(i++, tokens);
      ps.setLong(i++, costMicros);
      ps.setLong(i, calls);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot adjust budget " + key, e);
    }
  }

  @Override
  public BudgetUsage usage(BudgetKey key) {
    try (Connection c = connection();
        PreparedStatement ps = c.prepareStatement(USAGE)) {
      bindKey(ps, key);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return BudgetUsage.ZERO;
        }
        return new BudgetUsage(rs.getLong(1), rs.getLong(2), rs.getLong(3));
      }
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot read budget " + key, e);
    }
  }

  /** Deletes counters of windows that started before {@code periodStartBefore}. */
  public int purgeWindowsBefore(Instant periodStartBefore) {
    try (Connection c = connection();
        PreparedStatement ps = c.prepareStatement(PURGE)) {
      ps.setTimestamp(1, Timestamp.from(periodStartBefore));
      return ps.executeUpdate();
    } catch (SQLException e) {
      throw new LedgerStoreException("cannot purge budget windows", e);
    }
  }

  private Connection connection() throws SQLException {
    Connection c = dataSource.getConnection();
    if (!c.getAutoCommit()) {
      c.setAutoCommit(true);
    }
    return c;
  }

  private static int bindKey(PreparedStatement ps, BudgetKey key) throws SQLException {
    ps.setString(1, key.pipeline());
    ps.setString(2, key.tenant());
    ps.setTimestamp(3, Timestamp.from(key.periodStart()));
    return 4;
  }
}
