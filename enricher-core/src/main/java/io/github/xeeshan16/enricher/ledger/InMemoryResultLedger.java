package io.github.xeeshan16.enricher.ledger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A {@link ResultLedger} in process memory.
 *
 * <p>It protects against duplicate paid calls within one JVM (container retries, re-deliveries
 * after a local rebalance) but not across instances or restarts. Use the PostgreSQL ledger for
 * that. Results are kept until purged.
 */
public final class InMemoryResultLedger implements ResultLedger {

  private final Clock clock;
  private final String owner;
  private final Map<LedgerKey, Slot> slots = new HashMap<>();

  public InMemoryResultLedger() {
    this(Clock.systemUTC(), "local");
  }

  public InMemoryResultLedger(Clock clock, String owner) {
    this.clock = Objects.requireNonNull(clock, "clock");
    this.owner = Objects.requireNonNull(owner, "owner");
  }

  private record Slot(
      String token, Instant leaseExpiresAt, LedgerEntry entry, Instant completedAt, long hits) {}

  @Override
  public synchronized Claim claim(LedgerKey key, Duration leaseTtl) {
    Instant now = clock.instant();
    Slot slot = slots.get(key);
    if (slot != null && slot.entry() != null) {
      slots.put(
          key,
          new Slot(null, null, slot.entry(), slot.completedAt(), Math.addExact(slot.hits(), 1)));
      return new Claim.Hit(slot.entry());
    }
    if (slot != null && slot.leaseExpiresAt().isAfter(now)) {
      return new Claim.Busy(owner, slot.leaseExpiresAt());
    }
    String token = UUID.randomUUID().toString();
    slots.put(key, new Slot(token, now.plus(leaseTtl), null, null, 0));
    return new Claim.Acquired(token);
  }

  @Override
  public synchronized LedgerEntry complete(LedgerKey key, LedgerEntry entry) {
    Objects.requireNonNull(entry, "entry");
    Slot slot = slots.get(key);
    if (slot != null && slot.entry() != null) {
      return slot.entry();
    }
    slots.put(key, new Slot(null, null, entry, clock.instant(), 0));
    return entry;
  }

  @Override
  public synchronized void release(LedgerKey key, String token) {
    Slot slot = slots.get(key);
    if (slot != null && slot.entry() == null && Objects.equals(slot.token(), token)) {
      slots.remove(key);
    }
  }

  @Override
  public synchronized int purgeCompletedBefore(Instant completedBefore) {
    int before = slots.size();
    slots
        .values()
        .removeIf(s -> s.completedAt() != null && s.completedAt().isBefore(completedBefore));
    return before - slots.size();
  }

  /** How many claims were answered from a stored result for {@code key}; for tests. */
  public synchronized long hits(LedgerKey key) {
    Slot slot = slots.get(key);
    return slot == null ? 0 : slot.hits();
  }
}
