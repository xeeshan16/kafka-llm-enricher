package io.github.xeeshan16.enricher.eval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** The arithmetic of the eval report, kept separate so it can be unit tested. */
public final class EvalMetrics {

  private EvalMetrics() {}

  /** An expected and a predicted label; {@code predicted} is {@code null} when there was none. */
  public record Pair(String expected, String predicted) {
    public Pair {
      Objects.requireNonNull(expected, "expected");
    }

    boolean correct() {
      return expected.equals(predicted);
    }
  }

  /** Share of pairs whose prediction equals the label; 0 for no pairs. */
  public static double accuracy(Collection<Pair> pairs) {
    if (pairs.isEmpty()) {
      return 0;
    }
    return (double) pairs.stream().filter(Pair::correct).count() / pairs.size();
  }

  /**
   * Unweighted mean of per-class F1 over every class that occurs as a label or a prediction. A
   * missing prediction counts as a miss for its label and as no class.
   */
  public static double macroF1(Collection<Pair> pairs) {
    TreeSet<String> classes = new TreeSet<>();
    for (Pair p : pairs) {
      classes.add(p.expected());
      if (p.predicted() != null) {
        classes.add(p.predicted());
      }
    }
    if (classes.isEmpty()) {
      return 0;
    }
    double sum = 0;
    for (String c : classes) {
      long tp = 0;
      long fp = 0;
      long fn = 0;
      for (Pair p : pairs) {
        boolean predicted = c.equals(p.predicted());
        boolean expected = c.equals(p.expected());
        if (predicted && expected) {
          tp++;
        } else if (predicted) {
          fp++;
        } else if (expected) {
          fn++;
        }
      }
      sum += tp == 0 ? 0 : 2.0 * tp / (2.0 * tp + fp + fn);
    }
    return sum / classes.size();
  }

  /** Counts by expected label, then predicted label ({@code -} when there was no prediction). */
  public static Map<String, Map<String, Long>> confusion(Collection<Pair> pairs) {
    Map<String, Map<String, Long>> out = new TreeMap<>();
    for (Pair p : pairs) {
      out.computeIfAbsent(p.expected(), k -> new TreeMap<>())
          .merge(p.predicted() == null ? "-" : p.predicted(), 1L, Long::sum);
    }
    return out;
  }

  /** Nearest-rank percentile ({@code q} in 0..100) of {@code values}; 0 for no values. */
  public static double percentile(Collection<Double> values, double q) {
    if (values.isEmpty()) {
      return 0;
    }
    if (q < 0 || q > 100) {
      throw new IllegalArgumentException("percentile must be between 0 and 100: " + q);
    }
    List<Double> sorted = new ArrayList<>(values);
    sorted.sort(null);
    int rank = (int) Math.ceil(q / 100.0 * sorted.size());
    return sorted.get(Math.max(0, rank - 1));
  }

  /** {@code part / whole}, 0 when {@code whole} is 0. */
  public static double share(long part, long whole) {
    return whole == 0 ? 0 : (double) part / whole;
  }
}
