package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetStore;
import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantBudgets;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link CostPolicy} of every pipeline, built once at startup from {@code
 * enricher.pipelines.<name>.tenant}, {@code .skip} and {@code .budget}. Invalid expressions or
 * contradicting budget settings fail the application with the pipeline name in the message.
 */
public final class CostPolicies {

  private static final Logger log = LoggerFactory.getLogger(CostPolicies.class);

  private final Map<String, CostPolicy> policies;

  public CostPolicies(Map<String, CostPolicy> policies) {
    this.policies = Map.copyOf(policies);
  }

  public static CostPolicies none() {
    return new CostPolicies(Map.of());
  }

  /** The policy of {@code pipeline}; {@link CostPolicy#none()} when it configures nothing. */
  public CostPolicy policy(String pipeline) {
    return policies.getOrDefault(pipeline, CostPolicy.none());
  }

  static CostPolicies from(
      EnricherProperties properties, BudgetStore store, PriceTable prices, Clock clock) {
    Map<String, CostPolicy> policies = new HashMap<>();
    for (Map.Entry<String, EnricherProperties.Pipeline> entry : properties.pipelines().entrySet()) {
      String name = entry.getKey();
      try {
        policies.put(name, build(name, entry.getValue(), store, prices, clock));
      } catch (RuntimeException e) {
        throw new IllegalStateException(
            "invalid pipeline 'enricher.pipelines." + name + "': " + e.getMessage(), e);
      }
    }
    return new CostPolicies(policies);
  }

  /** Whether the pipeline configures any cap. */
  static boolean hasBudget(EnricherProperties.Pipeline p) {
    EnricherProperties.Budget b = p.budget();
    return b != null && (b.maxTokens() != null || b.maxCostUsd() != null || !b.tenants().isEmpty());
  }

  /** Whether over-budget records of the pipeline can be parked on its deferred topic. */
  static boolean defers(EnricherProperties.Pipeline p) {
    if (!hasBudget(p)) {
      return false;
    }
    OverBudgetAction action = p.budget().onExceeded();
    return action == OverBudgetAction.DEFER
        || (action == OverBudgetAction.DOWNGRADE
            && p.budget().afterDowngrade() == OverBudgetAction.DEFER);
  }

  private static CostPolicy build(
      String name,
      EnricherProperties.Pipeline p,
      BudgetStore store,
      PriceTable prices,
      Clock clock) {
    List<SkipRule> rules = new ArrayList<>();
    p.skip()
        .forEach((rule, expression) -> rules.add(SpelRecordExpressions.skipRule(rule, expression)));
    TenantResolver tenants =
        p.tenant() == null || p.tenant().isBlank()
            ? TenantResolver.fixed(TenantResolver.DEFAULT_TENANT)
            : SpelRecordExpressions.tenantResolver(name, p.tenant());
    if (!hasBudget(p)) {
      return new CostPolicy(
          rules, tenants, null, OverBudgetAction.DEFER, null, OverBudgetAction.DEFER);
    }
    EnricherProperties.Budget b = p.budget();
    BudgetLimit defaults = new BudgetLimit(tokens(b.maxTokens()), micros(b.maxCostUsd()));
    Map<String, BudgetLimit> perTenant = new LinkedHashMap<>();
    b.tenants()
        .forEach(
            (tenant, t) ->
                perTenant.put(
                    tenant,
                    new BudgetLimit(
                        t.maxTokens() == null ? defaults.maxTokens() : tokens(t.maxTokens()),
                        t.maxCostUsd() == null
                            ? defaults.maxCostMicros()
                            : micros(t.maxCostUsd()))));
    boolean costCapped =
        defaults.maxCostMicros() >= 0
            || perTenant.values().stream().anyMatch(l -> l.maxCostMicros() >= 0);
    if (costCapped) {
      warnIfUnpriced(name, p.model(), prices);
      if (b.onExceeded() == OverBudgetAction.DOWNGRADE && b.fallbackModel() != null) {
        warnIfUnpriced(name, b.fallbackModel(), prices);
      }
    }
    TenantBudgets budgets =
        new TenantBudgets(store, prices, b.period(), defaults, perTenant, clock);
    return new CostPolicy(
        rules, tenants, budgets, b.onExceeded(), b.fallbackModel(), b.afterDowngrade());
  }

  private static long tokens(Long value) {
    return value == null || value < 0 ? -1 : value;
  }

  private static long micros(BigDecimal usd) {
    return usd == null || usd.signum() < 0 ? -1 : PriceTable.toMicros(usd);
  }

  private static void warnIfUnpriced(String pipeline, String model, PriceTable prices) {
    if (!prices.knows(model)) {
      log.warn(
          "pipeline={} has a cost budget but model '{}' has no price under enricher.pricing; its"
              + " calls count as free and only token caps apply to them",
          pipeline,
          model);
    }
  }
}
