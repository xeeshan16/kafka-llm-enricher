package io.github.xeeshan16.enricher.ledger;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Remembers nothing: every claim is granted, so every delivery calls the provider. For pipelines
 * where calls are cheap and storing results is not allowed.
 */
public final class NoopResultLedger implements ResultLedger {

  @Override
  public Claim claim(LedgerKey key, Duration leaseTtl) {
    return new Claim.Acquired(UUID.randomUUID().toString());
  }

  @Override
  public LedgerEntry complete(LedgerKey key, LedgerEntry entry) {
    return entry;
  }

  @Override
  public void release(LedgerKey key, String token) {}

  @Override
  public int purgeCompletedBefore(Instant completedBefore) {
    return 0;
  }
}
