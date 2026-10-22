package io.github.xeeshan16.enricher.ledger;

import java.time.Instant;
import java.util.Objects;

/** Result of {@link ResultLedger#claim}. */
public sealed interface Claim {

  /** The enrichment was already paid for; use it instead of calling the provider. */
  record Hit(LedgerEntry entry) implements Claim {
    public Hit {
      Objects.requireNonNull(entry, "entry");
    }
  }

  /**
   * The caller holds the lease and should call the provider, then {@link ResultLedger#complete} or
   * {@link ResultLedger#release}.
   *
   * @param token identifies this lease; pass it to {@link ResultLedger#release}
   */
  record Acquired(String token) implements Claim {
    public Acquired {
      Objects.requireNonNull(token, "token");
    }
  }

  /**
   * Another worker holds a live lease for the same key and may be calling the provider right now.
   *
   * @param holder the lease owner, for diagnostics
   * @param leaseExpiresAt when the lease lapses and the key can be claimed again
   */
  record Busy(String holder, Instant leaseExpiresAt) implements Claim {}
}
