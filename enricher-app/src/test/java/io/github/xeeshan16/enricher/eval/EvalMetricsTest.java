package io.github.xeeshan16.enricher.eval;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import io.github.xeeshan16.enricher.eval.EvalMetrics.Pair;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EvalMetricsTest {

  @Test
  void accuracyCountsMissingPredictionsAsWrong() {
    List<Pair> pairs =
        List.of(new Pair("a", "a"), new Pair("a", "b"), new Pair("b", "b"), new Pair("b", null));
    assertThat(EvalMetrics.accuracy(pairs)).isEqualTo(0.5);
    assertThat(EvalMetrics.accuracy(List.of())).isZero();
  }

  @Test
  void macroF1AveragesPerClassF1() {
    // a: tp 1, fp 0, fn 1 -> F1 2/3. b: tp 1, fp 1, fn 0 -> F1 2/3. c: predicted never, label once
    // -> F1 0. Mean 4/9.
    List<Pair> pairs =
        List.of(new Pair("a", "a"), new Pair("a", "b"), new Pair("b", "b"), new Pair("c", null));
    assertThat(EvalMetrics.macroF1(pairs)).isCloseTo(4.0 / 9.0, within(1e-9));
    assertThat(EvalMetrics.macroF1(List.of(new Pair("x", "x")))).isEqualTo(1.0);
  }

  @Test
  void confusionGroupsByExpectedThenPredicted() {
    Map<String, Map<String, Long>> m =
        EvalMetrics.confusion(
            List.of(new Pair("a", "a"), new Pair("a", "b"), new Pair("a", "b"), new Pair("b", null)));
    assertThat(m).containsOnlyKeys("a", "b");
    assertThat(m.get("a")).containsEntry("a", 1L).containsEntry("b", 2L);
    assertThat(m.get("b")).containsEntry("-", 1L);
  }

  @Test
  void nearestRankPercentiles() {
    List<Double> values = List.of(5.0, 1.0, 4.0, 2.0, 3.0, 6.0, 7.0, 8.0, 9.0, 10.0);
    assertThat(EvalMetrics.percentile(values, 50)).isEqualTo(5.0);
    assertThat(EvalMetrics.percentile(values, 95)).isEqualTo(10.0);
    assertThat(EvalMetrics.percentile(values, 0)).isEqualTo(1.0);
    assertThat(EvalMetrics.percentile(List.of(), 95)).isZero();
    assertThatThrownBy(() -> EvalMetrics.percentile(values, 101))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void datasetLoadsWithLabelsAndPii() {
    List<EvalCase> cases = EvalCase.load(EvalCase.DATASET, 0);
    assertThat(cases).hasSize(240);
    assertThat(cases).extracting(EvalCase::id).doesNotHaveDuplicates();
    assertThat(cases)
        .allSatisfy(
            c -> {
              assertThat(c.event().path("disputeId").asText()).isEqualTo(c.id());
              assertThat(c.urgency()).isIn("low", "medium", "high");
              c.pii().forEach(p -> assertThat(c.json()).contains(p));
            });
    assertThat(cases).filteredOn(c -> c.has("injection")).isNotEmpty();
    assertThat(EvalCase.load(EvalCase.DATASET, 10)).hasSize(10);

    EvalCase first = cases.getFirst();
    assertThat(first.reorderedJson()).isNotEqualTo(first.json());
    assertThat(first.reorderedJson()).startsWith("{\"note\"");
  }
}
