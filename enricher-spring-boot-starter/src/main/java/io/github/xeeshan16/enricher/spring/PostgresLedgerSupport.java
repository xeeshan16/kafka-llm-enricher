package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.ledger.postgres.LedgerSchema;
import io.github.xeeshan16.enricher.ledger.postgres.PostgresResultLedger;
import javax.sql.DataSource;

/**
 * Creates the PostgreSQL ledger. Kept in its own class so that the auto-configuration loads the
 * optional {@code enricher-ledger-postgres} module only when it is on the classpath.
 */
final class PostgresLedgerSupport {

  static final String LEDGER_CLASS =
      "io.github.xeeshan16.enricher.ledger.postgres.PostgresResultLedger";

  private PostgresLedgerSupport() {}

  static ResultLedger create(DataSource dataSource, String owner, boolean initializeSchema) {
    if (initializeSchema) {
      LedgerSchema.migrate(dataSource);
    }
    return new PostgresResultLedger(dataSource, owner);
  }
}
