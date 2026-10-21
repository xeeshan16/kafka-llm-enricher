package io.github.xeeshan16.enricher.llm.fake;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.TokenEstimator;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Deterministic, offline {@link LlmProvider} for tests, CI and the demo.
 *
 * <p>The same request always yields the same text, so pipelines built on it are reproducible. By
 * default it answers with {@link SchemaAwareResponder}; tests can pass their own responder to
 * script failures such as malformed JSON. Token usage is estimated with {@link TokenEstimator}. It
 * also counts calls, which lets tests assert how many paid calls a real provider would have
 * received.
 */
public final class FakeLlmProvider implements LlmProvider {

  /** Model name the fake reports when a pipeline does not set one. */
  public static final String DEFAULT_MODEL = "fake-1";

  private final Function<LlmRequest, String> responder;
  private final AtomicLong calls = new AtomicLong();

  public FakeLlmProvider() {
    this(new SchemaAwareResponder());
  }

  public FakeLlmProvider(Function<LlmRequest, String> responder) {
    this.responder = Objects.requireNonNull(responder, "responder");
  }

  @Override
  public String name() {
    return "fake";
  }

  @Override
  public LlmResponse complete(LlmRequest request) {
    calls.incrementAndGet();
    String text = Objects.requireNonNull(responder.apply(request), "responder returned null");
    TokenUsage usage =
        new TokenUsage(
            TokenEstimator.estimate(request.systemPrompt())
                + TokenEstimator.estimate(request.userPrompt()),
            TokenEstimator.estimate(text));
    return new LlmResponse(text, request.model(), usage, Duration.ZERO);
  }

  /** Number of {@link #complete} calls so far. */
  public long calls() {
    return calls.get();
  }
}
