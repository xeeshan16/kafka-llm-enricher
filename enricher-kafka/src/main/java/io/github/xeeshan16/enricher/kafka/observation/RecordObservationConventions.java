package io.github.xeeshan16.enricher.kafka.observation;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/**
 * Names and tags of record observations, after the OpenTelemetry messaging semantic conventions.
 *
 * <ul>
 *   <li>{@value #PROCESS}: a timer and a {@code process <topic>} consumer span per input record.
 *       Tags: {@code messaging.system}, {@code messaging.operation.name}, {@code
 *       messaging.destination.name}, {@code pipeline}, {@code outcome} and {@code error.type};
 *       spans also get partition, offset and tenant.
 *   <li>{@value #PUBLISH}: a {@code send <topic>} producer span per output record whose context is
 *       written into the output's headers. Tags as above, {@code outcome} being the record's route.
 *       It ends when the record is handed to the engine, before the batch's transaction commits.
 * </ul>
 *
 * <p>Record keys and values are never attached.
 */
public final class RecordObservationConventions {

  public static final String PROCESS = "enricher.record.process";
  public static final String PUBLISH = "enricher.record.publish";

  private RecordObservationConventions() {}

  /** Default convention of {@link #PROCESS} observations. */
  public static final class Process implements ObservationConvention<RecordProcessContext> {

    public static final Process INSTANCE = new Process();

    @Override
    public boolean supportsContext(Observation.Context context) {
      return context instanceof RecordProcessContext;
    }

    @Override
    public String getName() {
      return PROCESS;
    }

    @Override
    public String getContextualName(RecordProcessContext context) {
      return "process " + context.record().topic();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(RecordProcessContext context) {
      Throwable error = context.getError();
      return KeyValues.of(
          KeyValue.of("messaging.system", "kafka"),
          KeyValue.of("messaging.operation.name", "process"),
          KeyValue.of("messaging.destination.name", context.record().topic()),
          KeyValue.of("pipeline", context.pipeline()),
          KeyValue.of("outcome", context.outcome()),
          KeyValue.of("error.type", error == null ? "none" : error.getClass().getSimpleName()));
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(RecordProcessContext context) {
      KeyValues values =
          KeyValues.of(
              KeyValue.of(
                  "messaging.destination.partition.id",
                  Integer.toString(context.record().partition())),
              KeyValue.of("messaging.kafka.offset", Long.toString(context.record().offset())));
      return context.tenant() == null
          ? values
          : values.and(KeyValue.of("enricher.tenant", context.tenant()));
    }
  }

  /** Default convention of {@link #PUBLISH} observations. */
  public static final class Publish implements ObservationConvention<RecordPublishContext> {

    public static final Publish INSTANCE = new Publish();

    @Override
    public boolean supportsContext(Observation.Context context) {
      return context instanceof RecordPublishContext;
    }

    @Override
    public String getName() {
      return PUBLISH;
    }

    @Override
    public String getContextualName(RecordPublishContext context) {
      return "send " + context.getCarrier().topic();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(RecordPublishContext context) {
      return KeyValues.of(
          KeyValue.of("messaging.system", "kafka"),
          KeyValue.of("messaging.operation.name", "send"),
          KeyValue.of("messaging.destination.name", context.getCarrier().topic()),
          KeyValue.of("pipeline", context.pipeline()),
          KeyValue.of("outcome", context.route()),
          KeyValue.of("error.type", "none"));
    }
  }
}
