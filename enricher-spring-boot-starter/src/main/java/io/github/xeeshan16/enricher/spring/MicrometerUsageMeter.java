package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.UsageMeter;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;

/**
 * Exports the engine's spend events as Micrometer meters.
 *
 * <table>
 *   <caption>Meters (Prometheus names in brackets)</caption>
 *   <tr><td>{@code gen_ai.client.token.usage} [{@code gen_ai_client_token_usage}]</td>
 *       <td>tokens per paid call, by {@code gen_ai.token.type} (input, output), model, provider
 *       and pipeline; buckets as in the GenAI semantic conventions</td></tr>
 *   <tr><td>{@code enricher.provider.calls}</td><td>paid calls by pipeline, tenant, model</td></tr>
 *   <tr><td>{@code enricher.tokens}</td><td>tokens by pipeline, tenant, model and type</td></tr>
 *   <tr><td>{@code enricher.cost} [{@code enricher_cost_usd_total}]</td>
 *       <td>USD spent, from {@code enricher.pricing}</td></tr>
 *   <tr><td>{@code enricher.ledger.hits}, {@code enricher.tokens.saved}, {@code
 *       enricher.cost.saved}</td><td>records answered from the result ledger and what they would
 *       have cost</td></tr>
 *   <tr><td>{@code enricher.records.skipped}, {@code .deferred}, {@code .downgraded}</td>
 *       <td>cost-control decisions, skips by reason</td></tr>
 *   <tr><td>{@code enricher.repairs}</td><td>repair prompts by {@code result} (fixed,
 *       failed)</td></tr>
 *   <tr><td>{@code enricher.pii.masked}</td><td>values masked in prompts by {@code type}</td></tr>
 * </table>
 *
 * <p>Tenant tags are bounded by {@link TenantTags}.
 */
public final class MicrometerUsageMeter implements UsageMeter {

  private static final double[] TOKEN_BUCKETS = {
    1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576
  };

  private final MeterRegistry registry;
  private final PriceTable prices;
  private final String provider;
  private final TenantTags tenants;

  /**
   * @param provider {@code gen_ai.provider.name} of the token usage histogram
   * @param tenantTag tag with the tenant, or {@code all}
   * @param maxTenants distinct tenant values before {@code other}
   */
  public MicrometerUsageMeter(
      MeterRegistry registry,
      PriceTable prices,
      String provider,
      boolean tenantTag,
      int maxTenants) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.prices = Objects.requireNonNull(prices, "prices");
    this.provider = Objects.requireNonNull(provider, "provider");
    this.tenants = new TenantTags(tenantTag, maxTenants);
  }

  @Override
  public void providerCall(String pipeline, String tenant, String model, TokenUsage usage) {
    String t = tenants.tag(tenant);
    counter("enricher.provider.calls", pipeline, t, model).increment();
    tokenUsage(pipeline, model, "input").record(usage.inputTokens());
    tokenUsage(pipeline, model, "output").record(usage.outputTokens());
    tokens("enricher.tokens", pipeline, t, model, "input").increment(usage.inputTokens());
    tokens("enricher.tokens", pipeline, t, model, "output").increment(usage.outputTokens());
    usd("enricher.cost", pipeline, t, model).increment(prices.costMicros(model, usage) / 1e6);
  }

  @Override
  public void ledgerHit(String pipeline, String tenant, String model, TokenUsage usage) {
    String t = tenants.tag(tenant);
    counter("enricher.ledger.hits", pipeline, t, model).increment();
    counter("enricher.tokens.saved", pipeline, t, model).increment(usage.totalTokens());
    usd("enricher.cost.saved", pipeline, t, model).increment(prices.costMicros(model, usage) / 1e6);
  }

  @Override
  public void skipped(String pipeline, String tenant, String reason) {
    Counter.builder("enricher.records.skipped")
        .description("Records not sent to the model, by skip rule or over-budget")
        .tags("pipeline", pipeline, "tenant", tenants.tag(tenant), "reason", reason)
        .register(registry)
        .increment();
  }

  @Override
  public void deferred(String pipeline, String tenant) {
    Counter.builder("enricher.records.deferred")
        .description("Records parked until their tenant's budget window resets")
        .tags("pipeline", pipeline, "tenant", tenants.tag(tenant))
        .register(registry)
        .increment();
  }

  @Override
  public void downgraded(String pipeline, String tenant, String fromModel, String toModel) {
    Counter.builder("enricher.records.downgraded")
        .description("Records routed to the fallback model because their tenant was over budget")
        .tags(
            "pipeline",
            pipeline,
            "tenant",
            tenants.tag(tenant),
            "from_model",
            fromModel,
            "to_model",
            toModel)
        .register(registry)
        .increment();
  }

  @Override
  public void repairAttempt(String pipeline, String tenant, String model, boolean fixed) {
    Counter.builder("enricher.repairs")
        .description("Repair prompts sent for unusable answers")
        .tags("pipeline", pipeline, "model", model, "result", fixed ? "fixed" : "failed")
        .register(registry)
        .increment();
  }

  @Override
  public void piiMasked(String pipeline, String label, int count) {
    Counter.builder("enricher.pii.masked")
        .description("Distinct personal-data values replaced by placeholders in prompts")
        .tags("pipeline", pipeline, "type", label)
        .register(registry)
        .increment(count);
  }

  private Counter counter(String name, String pipeline, String tenant, String model) {
    return Counter.builder(name)
        .tags("pipeline", pipeline, "tenant", tenant, "model", model)
        .register(registry);
  }

  private Counter tokens(String name, String pipeline, String tenant, String model, String type) {
    return Counter.builder(name)
        .tags("pipeline", pipeline, "tenant", tenant, "model", model, "type", type)
        .register(registry);
  }

  private Counter usd(String name, String pipeline, String tenant, String model) {
    return Counter.builder(name)
        .baseUnit("usd")
        .tags("pipeline", pipeline, "tenant", tenant, "model", model)
        .register(registry);
  }

  private DistributionSummary tokenUsage(String pipeline, String model, String type) {
    return DistributionSummary.builder("gen_ai.client.token.usage")
        .description("Tokens used per model call")
        .serviceLevelObjectives(TOKEN_BUCKETS)
        .tags(
            "gen_ai.operation.name",
            "chat",
            "gen_ai.provider.name",
            provider,
            "gen_ai.request.model",
            model,
            "gen_ai.token.type",
            type,
            "pipeline",
            pipeline)
        .register(registry);
  }
}
