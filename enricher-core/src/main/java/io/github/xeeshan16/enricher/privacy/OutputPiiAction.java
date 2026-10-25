package io.github.xeeshan16.enricher.privacy;

/**
 * What happens when a validated answer contains personal data. The masker only removes what the
 * prompt contained, so personal data in an answer was either missed by the detectors, invented by
 * the model, or pulled from a field that was deliberately left unmasked.
 */
public enum OutputPiiAction {
  /** Publish the answer as it is. */
  ALLOW,
  /**
   * Replace the personal data with placeholders and validate again; an answer that no longer fits
   * the schema fails as a schema violation.
   */
  MASK,
  /** Dead-letter the record with reason {@code PII_IN_OUTPUT}. */
  REJECT
}
