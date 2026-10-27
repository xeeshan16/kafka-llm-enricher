package io.github.xeeshan16.enricher.eval;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wraps the configured provider and records every call that reaches it: which dispute it was for,
 * whether it was a repair prompt, the exact prompts sent, how long it took and whether it failed.
 */
public final class RecordingLlmProvider implements LlmProvider {

  private static final Pattern DISPUTE_ID = Pattern.compile("\"disputeId\"\\s*:\\s*\"(E-\\d+)\"");

  /** One call as the provider saw it. */
  public record Call(
      String disputeId,
      boolean repair,
      String systemPrompt,
      String userPrompt,
      long nanos,
      boolean failed,
      int phase) {}

  private final LlmProvider delegate;
  private final List<Call> calls = new CopyOnWriteArrayList<>();
  private volatile int phase = 1;

  public RecordingLlmProvider(LlmProvider delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  public LlmProvider delegate() {
    return delegate;
  }

  /** Calls from now on are tagged with {@code phase} (1: first pass, 2: replay). */
  public void phase(int newPhase) {
    this.phase = newPhase;
  }

  public List<Call> calls() {
    return List.copyOf(calls);
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public LlmResponse complete(LlmRequest request) {
    String id = disputeId(request.userPrompt());
    boolean repair = request.userPrompt().startsWith("Your previous answer");
    int current = phase;
    long start = System.nanoTime();
    try {
      LlmResponse response = delegate.complete(request);
      calls.add(
          new Call(
              id,
              repair,
              request.systemPrompt(),
              request.userPrompt(),
              System.nanoTime() - start,
              false,
              current));
      return response;
    } catch (RuntimeException e) {
      calls.add(
          new Call(
              id,
              repair,
              request.systemPrompt(),
              request.userPrompt(),
              System.nanoTime() - start,
              true,
              current));
      throw e;
    }
  }

  static String disputeId(String prompt) {
    Matcher m = DISPUTE_ID.matcher(prompt);
    return m.find() ? m.group(1) : "?";
  }
}
