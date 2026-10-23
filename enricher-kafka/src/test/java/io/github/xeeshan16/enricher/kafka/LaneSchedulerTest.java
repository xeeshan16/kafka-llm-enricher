package io.github.xeeshan16.enricher.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LaneSchedulerTest {

  private final LaneScheduler<String> scheduler = new LaneScheduler<>();

  @Test
  void oneTaskPerLaneAtATimeInOrder() {
    scheduler.add("a", "a1", 0, 0);
    scheduler.add("a", "a2", 0, 0);
    scheduler.add("b", "b1", 0, 0);

    assertThat(scheduler.next(0)).isEqualTo("a1");
    assertThat(scheduler.next(0)).isEqualTo("b1");
    assertThat(scheduler.next(0)).as("lane a is busy").isNull();
    assertThat(scheduler.queued()).isOne();
    assertThat(scheduler.running()).isEqualTo(2);

    scheduler.done("a", 0);
    assertThat(scheduler.next(0)).isEqualTo("a2");
    assertThat(scheduler.queued()).isZero();
  }

  @Test
  void delayedTaskWaitsForItsDueTimeAndHoldsItsLaneOnly() {
    scheduler.add("retry", "r1", 1_000, 0);
    scheduler.add("retry", "r2", 0, 0);
    scheduler.add("other", "o1", 0, 0);

    assertThat(scheduler.next(0)).isEqualTo("o1");
    assertThat(scheduler.next(0)).isNull();
    assertThat(scheduler.nextDueMillis()).isEqualTo(1_000);
    assertThat(scheduler.next(999)).isNull();
    assertThat(scheduler.next(1_000)).isEqualTo("r1");
    scheduler.done("retry", 1_000);
    assertThat(scheduler.next(1_000)).isEqualTo("r2");
  }

  @Test
  void addFirstRunsAgainBeforeLaterTasks() {
    scheduler.add("a", "a1", 0, 0);
    scheduler.add("a", "a2", 0, 0);
    assertThat(scheduler.next(0)).isEqualTo("a1");

    scheduler.addFirst("a", "a1-again", 500);
    scheduler.done("a", 0);

    assertThat(scheduler.next(0)).isNull();
    assertThat(scheduler.next(500)).isEqualTo("a1-again");
    scheduler.done("a", 500);
    assertThat(scheduler.next(500)).isEqualTo("a2");
  }

  @Test
  void removedLanesAreForgottenIncludingStaleReadyEntries() {
    scheduler.add("p0:a", "a1", 0, 0);
    scheduler.add("p0:a", "a2", 0, 0);
    scheduler.add("p1:b", "b1", 0, 0);
    assertThat(scheduler.next(0)).isEqualTo("a1");

    scheduler.removeLanes(lane -> ((String) lane).startsWith("p0"));
    scheduler.done("p0:a", 0);

    assertThat(scheduler.queued()).isOne();
    assertThat(scheduler.next(0)).isEqualTo("b1");
    assertThat(scheduler.next(0)).isNull();
  }
}
