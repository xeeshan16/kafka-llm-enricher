package io.github.xeeshan16.enricher.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Creates the Kafka clients of a {@link ConsumerLoop}. Each loop owns one consumer and one producer
 * and uses both from its poll thread only.
 */
public interface KafkaClientFactory {

  /**
   * A consumer in {@code groupId}. The loop commits offsets itself, so auto-commit must be off; for
   * exactly-once chains, use {@code isolation.level=read_committed}.
   */
  Consumer<String, String> createConsumer(String groupId, String clientId);

  /**
   * A producer. With a non-null {@code transactionalId} it must be transactional (idempotence on);
   * the loop calls {@code initTransactions()} itself. Transactional ids must be unique per loop and
   * stable across restarts of the same instance.
   */
  Producer<String, String> createProducer(String clientId, String transactionalId);
}
