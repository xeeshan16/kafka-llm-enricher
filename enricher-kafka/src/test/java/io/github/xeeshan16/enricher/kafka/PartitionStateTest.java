package io.github.xeeshan16.enricher.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class PartitionStateTest {

  private final PartitionState state = new PartitionState(new TopicPartition("in", 0));

  private static ConsumerRecord<String, String> record(long offset) {
    return new ConsumerRecord<>("in", 0, offset, "k", "v" + offset);
  }

  private static List<ProducerRecord<String, String>> out(long offset) {
    return List.of(new ProducerRecord<>("out", "k", "o" + offset));
  }

  @Test
  void commitsOnlyTheFinishedPrefixInOffsetOrder() {
    for (long offset = 10; offset < 14; offset++) {
      assertThat(state.add(record(offset))).isTrue();
    }
    state.complete(12, out(12));
    state.complete(11, out(11));

    List<ProducerRecord<String, String>> sink = new ArrayList<>();
    assertThat(state.drainDone(sink)).as("offset 10 still running").isEqualTo(-1);
    assertThat(sink).isEmpty();

    state.complete(10, out(10));
    assertThat(state.drainDone(sink)).isEqualTo(13);
    assertThat(sink).extracting(ProducerRecord::value).containsExactly("o10", "o11", "o12");
    assertThat(state.pending()).isOne();
    assertThat(state.firstOffset()).isEqualTo(13);
  }

  @Test
  void ignoresDuplicateAndOlderOffsetsAndUnknownCompletions() {
    state.add(record(5));
    assertThat(state.add(record(5))).isFalse();
    assertThat(state.add(record(4))).isFalse();
    state.complete(99, out(99));

    assertThat(state.pending()).isOne();
    assertThat(state.drainDone(new ArrayList<>())).isEqualTo(-1);
  }

  @Test
  void emptyOutputStillAdvancesTheOffset() {
    state.add(record(0));
    state.complete(0, List.of());

    assertThat(state.drainDone(new ArrayList<>())).isEqualTo(1);
    assertThat(state.firstOffset()).isEqualTo(-1);
  }
}
