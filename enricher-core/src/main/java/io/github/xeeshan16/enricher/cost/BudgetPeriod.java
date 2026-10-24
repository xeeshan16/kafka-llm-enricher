package io.github.xeeshan16.enricher.cost;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/** The window a budget applies to. Windows are calendar based, in UTC. */
public enum BudgetPeriod {
  /** From 00:00 UTC to the next 00:00 UTC. */
  DAILY,
  /** From the first of the month, 00:00 UTC, to the first of the next month. */
  MONTHLY;

  /** Start of the window containing {@code instant}. */
  public Instant start(Instant instant) {
    return switch (this) {
      case DAILY -> instant.truncatedTo(ChronoUnit.DAYS);
      case MONTHLY ->
          LocalDate.ofInstant(instant, ZoneOffset.UTC)
              .withDayOfMonth(1)
              .atStartOfDay(ZoneOffset.UTC)
              .toInstant();
    };
  }

  /** Start of the window after the one containing {@code instant}: when the budget resets. */
  public Instant next(Instant instant) {
    Instant start = start(instant);
    return switch (this) {
      case DAILY -> start.plus(1, ChronoUnit.DAYS);
      case MONTHLY ->
          LocalDate.ofInstant(start, ZoneOffset.UTC)
              .plusMonths(1)
              .atStartOfDay(ZoneOffset.UTC)
              .toInstant();
    };
  }
}
