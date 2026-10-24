package io.github.xeeshan16.enricher.cost;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class BudgetPeriodTest {

  @Test
  void dailyWindowsAreUtcDays() {
    Instant t = Instant.parse("2026-10-24T23:59:59Z");
    assertThat(BudgetPeriod.DAILY.start(t)).isEqualTo(Instant.parse("2026-10-24T00:00:00Z"));
    assertThat(BudgetPeriod.DAILY.next(t)).isEqualTo(Instant.parse("2026-10-25T00:00:00Z"));
  }

  @Test
  void monthlyWindowsAreCalendarMonths() {
    Instant t = Instant.parse("2026-12-31T12:00:00Z");
    assertThat(BudgetPeriod.MONTHLY.start(t)).isEqualTo(Instant.parse("2026-12-01T00:00:00Z"));
    assertThat(BudgetPeriod.MONTHLY.next(t)).isEqualTo(Instant.parse("2027-01-01T00:00:00Z"));
    assertThat(BudgetPeriod.MONTHLY.next(Instant.parse("2028-02-29T00:00:00Z")))
        .isEqualTo(Instant.parse("2028-03-01T00:00:00Z"));
  }
}
