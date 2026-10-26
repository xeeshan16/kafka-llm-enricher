package io.github.xeeshan16.enricher.app.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Publishes {@link SyntheticDisputes} at a fixed rate on its own thread, with a plain idempotent
 * producer (no transactions: it plays the upstream service, not the enricher).
 */
public final class DisputeGenerator implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(DisputeGenerator.class);

  private final DemoGeneratorProperties properties;
  private final Map<String, Object> producerConfig;
  private volatile boolean running;
  private volatile Thread thread;
  private final AtomicLong published = new AtomicLong();

  /**
   * @param producerConfig Kafka client settings (bootstrap servers, security); serialisers,
   *     idempotence and the client id are set here, and any transactional id is removed
   */
  public DisputeGenerator(DemoGeneratorProperties properties, Map<String, Object> producerConfig) {
    this.properties = Objects.requireNonNull(properties, "properties");
    Map<String, Object> config = new HashMap<>(producerConfig);
    config.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "dispute-generator");
    this.producerConfig = config;
  }

  /** Events published so far. */
  public long published() {
    return published.get();
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    running = true;
    thread = Thread.ofPlatform().name("dispute-generator").daemon(true).start(this::run);
    log.info(
        "demo generator publishing to {} at {}/s (max {}, replay ratio {}, seed {})",
        properties.topic(),
        properties.eventsPerSecond(),
        properties.maxEvents() == 0 ? "unlimited" : properties.maxEvents(),
        properties.replayRatio(),
        properties.seed());
  }

  private void run() {
    SyntheticDisputes disputes = new SyntheticDisputes(properties.seed(), properties.replayRatio());
    long intervalNanos = (long) (TimeUnit.SECONDS.toNanos(1) / properties.eventsPerSecond());
    long next = System.nanoTime();
    try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
      while (running && (properties.maxEvents() == 0 || published.get() < properties.maxEvents())) {
        SyntheticDisputes.Dispute dispute = disputes.next();
        producer.send(
            new ProducerRecord<>(properties.topic(), dispute.key(), dispute.json()),
            (metadata, error) -> {
              if (error != null) {
                log.warn("demo generator could not publish: {}", error.toString());
              }
            });
        long count = published.incrementAndGet();
        if (count % 500 == 0) {
          log.info("demo generator published {} disputes", count);
        }
        next += intervalNanos;
        long wait = next - System.nanoTime();
        if (wait > 0) {
          TimeUnit.NANOSECONDS.sleep(wait);
        }
      }
      producer.flush();
      log.info("demo generator stopped after {} disputes", published.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      log.error("demo generator failed", e);
    } finally {
      running = false;
    }
  }

  @Override
  public synchronized void stop() {
    running = false;
    Thread t = thread;
    if (t != null) {
      try {
        t.join(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /** Starts after the pipelines, stops before them. */
  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 100;
  }
}
