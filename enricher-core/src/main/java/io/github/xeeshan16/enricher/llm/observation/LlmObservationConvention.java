package io.github.xeeshan16.enricher.llm.observation;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/**
 * Names and tags of model-call observations. Define a bean of this type (or pass one to {@link
 * ObservedLlmProvider}) to add or change tags; extend {@link DefaultLlmObservationConvention} to
 * keep the defaults.
 */
public interface LlmObservationConvention extends ObservationConvention<LlmCallContext> {

  @Override
  default boolean supportsContext(Observation.Context context) {
    return context instanceof LlmCallContext;
  }
}
