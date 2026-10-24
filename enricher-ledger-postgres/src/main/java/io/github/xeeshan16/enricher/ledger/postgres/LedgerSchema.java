package io.github.xeeshan16.enricher.ledger.postgres;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and upgrades the ledger and budget tables with Flyway.
 *
 * <p>The ledger keeps its own migration history table ({@value #HISTORY_TABLE}) and its own
 * migration location ({@value #LOCATION}), so it never interferes with an application's own Flyway
 * or Liquibase migrations in the same schema. Concurrent instances are safe: Flyway takes a
 * PostgreSQL advisory lock while migrating. Teams that apply DDL through their own tooling can copy
 * the SQL from {@value #LOCATION} and turn this off.
 */
public final class LedgerSchema {

  public static final String LOCATION = "classpath:db/enricher-ledger";
  public static final String HISTORY_TABLE = "enricher_ledger_schema_history";

  private static final Logger log = LoggerFactory.getLogger(LedgerSchema.class);

  private LedgerSchema() {}

  /** Applies pending ledger migrations to the default schema of {@code dataSource}. */
  public static MigrateResult migrate(DataSource dataSource) {
    MigrateResult result =
        Flyway.configure()
            .dataSource(dataSource)
            .locations(LOCATION)
            .table(HISTORY_TABLE)
            // The schema usually holds the application's own tables already. Baseline at 0 so the
            // ledger's own V1 still runs.
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load()
            .migrate();
    log.info(
        "result ledger schema at version {} ({} migrations applied)",
        result.targetSchemaVersion == null ? "unchanged" : result.targetSchemaVersion,
        result.migrationsExecuted);
    return result;
  }
}
