package io.github.xeeshan16.enricher.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Non-blocking retry stages of a pipeline: {@code <input>.enricher-retry-1}, {@code -2}, ..., each
 * with its delay. A record whose retryable failure survives the in-place attempts moves to the next
 * stage with a due time of now plus that stage's delay, so the input partition keeps flowing. After
 * the last stage it goes to the dead-letter topic.
 *
 * <p>Trade-off: a record that moves to a retry topic no longer holds back later records with the
 * same key, so per-key order is kept for successful first attempts only. Configure no stages to
 * send exhausted records straight to the dead-letter topic instead.
 *
 * @param inputTopic the pipeline's input topic
 * @param delays delay before each retry stage; empty for no retry topics
 */
public record RetryTopics(String inputTopic, List<Duration> delays) {

  static final String SUFFIX = ".enricher-retry-";

  public RetryTopics {
    Objects.requireNonNull(inputTopic, "inputTopic");
    delays = List.copyOf(delays);
    for (Duration delay : delays) {
      if (delay.isNegative()) {
        throw new IllegalArgumentException("retry delays must not be negative: " + delay);
      }
    }
  }

  public static RetryTopics none(String inputTopic) {
    return new RetryTopics(inputTopic, List.of());
  }

  /** Number of retry stages. */
  public int stages() {
    return delays.size();
  }

  /** Topic of retry stage {@code stage} (1-based). */
  public String topic(int stage) {
    if (stage < 1 || stage > delays.size()) {
      throw new IllegalArgumentException("no retry stage " + stage);
    }
    return inputTopic + SUFFIX + stage;
  }

  /** Delay before retry stage {@code stage} (1-based) runs. */
  public Duration delay(int stage) {
    return delays.get(stage - 1);
  }

  /** The retry topics, in stage order. */
  public List<String> topics() {
    List<String> topics = new ArrayList<>();
    for (int stage = 1; stage <= delays.size(); stage++) {
      topics.add(topic(stage));
    }
    return topics;
  }

  /** The input topic followed by the retry topics: what the pipeline subscribes to. */
  public List<String> subscriptions() {
    List<String> all = new ArrayList<>();
    all.add(inputTopic);
    all.addAll(topics());
    return all;
  }

  /** 0 for the input topic, the stage for a retry topic, -1 for anything else. */
  public int stageOf(String topic) {
    if (topic.equals(inputTopic)) {
      return 0;
    }
    for (int stage = 1; stage <= delays.size(); stage++) {
      if (topic.equals(topic(stage))) {
        return stage;
      }
    }
    return -1;
  }
}
