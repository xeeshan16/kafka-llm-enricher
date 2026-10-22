package io.github.xeeshan16.enricher.ledger;

import java.time.Duration;
import java.time.Instant;

/**
 * Remembers which enrichments were already paid for, and stops two workers from paying for the same
 * one at the same time.
 *
 * <p>Protocol for one record:
 *
 * <ol>
 *   <li>{@link #claim}: returns a stored result ({@link Claim.Hit}), a lease ({@link
 *       Claim.Acquired}) or tells the caller that someone else holds the lease ({@link
 *       Claim.Busy}).
 *   <li>With a lease, call the provider. On a usable answer, {@link #complete}; otherwise {@link
 *       #release} so that a later attempt can claim the key at once.
 * </ol>
 *
 * <p>Leases expire, so a worker that dies while holding one delays its key by at most the lease
 * time and never blocks it for good. {@link #complete} is idempotent and the first completed result
 * wins: a worker whose lease lapsed and who completes late gets the winning result back, so every
 * output for one key carries the same answer.
 *
 * <p>Implementations must be thread-safe and, for multi-instance deployments, shared between
 * instances (see the PostgreSQL implementation).
 */
public interface ResultLedger {

  /** Tries to claim {@code key} for {@code leaseTtl}. */
  Claim claim(LedgerKey key, Duration leaseTtl);

  /**
   * Stores the result for {@code key} unless one is already stored, and returns the stored result.
   * The caller should publish the returned entry, which may differ from {@code entry} when another
   * worker completed first.
   */
  LedgerEntry complete(LedgerKey key, LedgerEntry entry);

  /** Drops the lease identified by {@code token}, if it is still held. Never throws for a lapse. */
  void release(LedgerKey key, String token);

  /** Deletes completed results older than {@code completedBefore}; returns how many. */
  int purgeCompletedBefore(Instant completedBefore);
}
