package io.github.xeeshan16.enricher.privacy;

import java.util.List;

/**
 * Finds one kind of personal data in a piece of text. An extension point: teams add detectors for
 * their own identifiers (customer numbers, national ids, account references) and pass them to
 * {@link PiiMasker}.
 *
 * <p>Implementations must be stateless and thread-safe, and should validate candidates (checksums,
 * lengths) rather than mask every long number: over-masking hides the facts the model needs.
 */
public interface PiiDetector {

  /** Label used in placeholders ({@code [<LABEL>_1]}) and metrics; upper case, no spaces. */
  String label();

  /** Non-overlapping spans of {@code text} to mask, in any order. */
  List<Span> find(String text);

  /**
   * A detected span.
   *
   * @param start inclusive start index
   * @param end exclusive end index
   */
  record Span(int start, int end) {
    public Span {
      if (start < 0 || end <= start) {
        throw new IllegalArgumentException("invalid span [" + start + ", " + end + ")");
      }
    }
  }
}
