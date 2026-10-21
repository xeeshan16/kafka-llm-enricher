package io.github.xeeshan16.enricher.llm;

/**
 * A chat-completion style model behind a stable, provider-neutral contract.
 *
 * <p>Implementations must be thread-safe: the engine calls {@link #complete} concurrently from
 * several consumer threads. Transport failures are reported as {@link LlmProviderException}, with
 * {@link LlmProviderException#retryable()} telling the caller whether a retry can help (timeouts,
 * 429s and 5xx responses) or not (bad requests, authentication errors).
 */
public interface LlmProvider {

  /** Short identifier used in metrics and logs, for example {@code fake} or {@code openai}. */
  String name();

  /** Runs one completion. Never returns {@code null}. */
  LlmResponse complete(LlmRequest request);
}
