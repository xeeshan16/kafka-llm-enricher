package io.github.xeeshan16.enricher.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import io.github.xeeshan16.enricher.engine.PreparedRecord;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

/**
 * Compiles the SpEL expressions of skip rules and tenant resolvers.
 *
 * <p>An expression sees one record as a map with these entries:
 *
 * <ul>
 *   <li>{@code event}: the parsed value, as nested maps and lists (a string for non-JSON values);
 *   <li>{@code key}, {@code topic}, {@code partition}, {@code offset}.
 * </ul>
 *
 * <p>Map entries read as properties ({@code event.merchant.country}); a missing entry reads as
 * {@code null} instead of failing, so {@code event.amount != null and event.amount < 5} is safe on
 * events without an amount. Indexing works too ({@code event['merchant-id']}).
 *
 * <p>Expressions run in a {@link SimpleEvaluationContext}: no type references ({@code T(...)}), no
 * constructors, no bean references, no assignment. Instance methods on the values are allowed
 * ({@code event.note.toLowerCase().contains('refund')}). Configuration is trusted input, but this
 * keeps a mistaken or tampered property from running arbitrary code inside the consumer.
 */
final class SpelRecordExpressions {

  private static final Logger log = LoggerFactory.getLogger(SpelRecordExpressions.class);
  private static final SpelExpressionParser PARSER = new SpelExpressionParser();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SpelRecordExpressions() {}

  /** Reads map entries as properties, with missing entries as {@code null}. Read-only. */
  static final class MapEntryAccessor implements PropertyAccessor {
    @Override
    public Class<?>[] getSpecificTargetClasses() {
      return new Class<?>[] {Map.class};
    }

    @Override
    public boolean canRead(EvaluationContext context, Object target, String name) {
      return target instanceof Map;
    }

    @Override
    public TypedValue read(EvaluationContext context, Object target, String name)
        throws AccessException {
      if (!(target instanceof Map<?, ?> map)) {
        throw new AccessException("not a map: " + target);
      }
      return new TypedValue(map.get(name));
    }

    @Override
    public boolean canWrite(EvaluationContext context, Object target, String name) {
      return false;
    }

    @Override
    public void write(EvaluationContext context, Object target, String name, Object newValue)
        throws AccessException {
      throw new AccessException("expressions are read-only");
    }
  }

  private static final EvaluationContext CONTEXT =
      SimpleEvaluationContext.forPropertyAccessors(new MapEntryAccessor())
          .withInstanceMethods()
          .build();

  static Expression parse(String expression, String what) {
    if (expression == null || expression.isBlank()) {
      throw new IllegalArgumentException(what + " must not be blank");
    }
    try {
      return PARSER.parseExpression(expression);
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          what + " is not a valid SpEL expression: " + e.getMessage(), e);
    }
  }

  /** The root object expressions are evaluated against. */
  static Map<String, Object> root(PreparedRecord record) {
    Map<String, Object> root = new HashMap<>();
    root.put("event", MAPPER.convertValue(record.input(), Object.class));
    root.put("key", record.record().key());
    root.put("topic", record.record().topic());
    root.put("partition", record.record().partition());
    root.put("offset", record.record().offset());
    return root;
  }

  /** A skip rule whose condition is {@code expression}; {@code null} results count as false. */
  static SkipRule skipRule(String name, String expression) {
    Expression compiled = parse(expression, "skip rule '" + name + "'");
    return new SkipRule(
        name,
        record -> {
          Boolean result = compiled.getValue(CONTEXT, root(record), Boolean.class);
          return Boolean.TRUE.equals(result);
        });
  }

  /**
   * A tenant resolver. A result that is null or blank, or an expression that fails on a record,
   * charges the record to {@link TenantResolver#DEFAULT_TENANT}; the first failure is logged.
   */
  static TenantResolver tenantResolver(String pipeline, String expression) {
    Expression compiled = parse(expression, "tenant expression");
    AtomicBoolean warned = new AtomicBoolean();
    return record -> {
      Object value;
      try {
        value = compiled.getValue(CONTEXT, root(record));
      } catch (RuntimeException e) {
        if (warned.compareAndSet(false, true)) {
          log.warn(
              "pipeline={} tenant expression '{}' failed ({}); charging such records to '{}'",
              pipeline,
              expression,
              e.getMessage(),
              TenantResolver.DEFAULT_TENANT);
        }
        return TenantResolver.DEFAULT_TENANT;
      }
      String tenant = Objects.toString(value, "");
      return tenant.isBlank() ? TenantResolver.DEFAULT_TENANT : tenant;
    };
  }
}
