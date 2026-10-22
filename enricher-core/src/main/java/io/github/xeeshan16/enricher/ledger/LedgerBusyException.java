package io.github.xeeshan16.enricher.ledger;

import java.time.Instant;

/**
 * Another worker holds the lease for this record's key and did not finish within the wait. The
 * record should be retried later, by which time the other worker has either completed (the retry is
 * a ledger hit) or its lease has lapsed (the retry claims the key).
 */
public class LedgerBusyException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final transient LedgerKey key;
  private final Instant leaseExpiresAt;

  public LedgerBusyException(LedgerKey key, String holder, Instant leaseExpiresAt) {
    super("ledger key " + key + " is leased by " + holder + " until " + leaseExpiresAt);
    this.key = key;
    this.leaseExpiresAt = leaseExpiresAt;
  }

  public LedgerKey key() {
    return key;
  }

  public Instant leaseExpiresAt() {
    return leaseExpiresAt;
  }
}
