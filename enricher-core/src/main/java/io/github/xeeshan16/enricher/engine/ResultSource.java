package io.github.xeeshan16.enricher.engine;

/** Where the enrichment of a published record came from. */
public enum ResultSource {
  /** A provider call made for this delivery. */
  PROVIDER,
  /** A result already paid for and read back from the result ledger. */
  LEDGER,
  /** No enrichment was produced: the record was skipped or deferred, and nothing was paid. */
  NONE
}
