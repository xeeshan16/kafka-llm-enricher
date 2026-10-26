package io.github.xeeshan16.enricher.spring;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns tenants into metric tag values with a bounded number of distinct values: the first {@code
 * maxTenants} tenants seen keep their own value, later ones share {@value #OTHER}. Concurrent first
 * sightings may admit a few more than the limit; the bound is what keeps a merchant-keyed tenant
 * from creating one time series per merchant.
 */
final class TenantTags {

  static final String ALL = "all";
  static final String OTHER = "other";
  static final String UNKNOWN = "unknown";

  private final boolean enabled;
  private final int maxTenants;
  private final Set<String> seen = ConcurrentHashMap.newKeySet();

  TenantTags(boolean enabled, int maxTenants) {
    this.enabled = enabled;
    this.maxTenants = maxTenants;
  }

  String tag(String tenant) {
    if (!enabled) {
      return ALL;
    }
    String value = tenant == null || tenant.isBlank() ? UNKNOWN : tenant;
    if (seen.contains(value)) {
      return value;
    }
    if (seen.size() < maxTenants) {
      seen.add(value);
      return value;
    }
    return OTHER;
  }
}
