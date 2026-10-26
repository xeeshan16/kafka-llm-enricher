package io.github.xeeshan16.enricher.app.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SyntheticDisputesTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static List<SyntheticDisputes.Dispute> take(SyntheticDisputes disputes, int n) {
    List<SyntheticDisputes.Dispute> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(disputes.next());
    }
    return out;
  }

  @Test
  void sameSeedSameStream() {
    assertThat(take(new SyntheticDisputes(7, 0.1), 200))
        .isEqualTo(take(new SyntheticDisputes(7, 0.1), 200));
    assertThat(take(new SyntheticDisputes(7, 0.1), 50))
        .isNotEqualTo(take(new SyntheticDisputes(8, 0.1), 50));
  }

  @Test
  void eventsAreWellFormedAndCarryOnlySyntheticPersonalData() throws Exception {
    List<SyntheticDisputes.Dispute> events = take(new SyntheticDisputes(42, 0), 1000);
    int withCard = 0;
    int withEmail = 0;
    for (SyntheticDisputes.Dispute d : events) {
      assertThat(d.key()).matches("card-1\\d{3}");
      JsonNode event = MAPPER.readTree(d.json());
      assertThat(event.path("disputeId").asText()).startsWith("D-");
      assertThat(SyntheticDisputes.MERCHANTS).contains(event.path("merchant").asText());
      assertThat(event.path("amount").decimalValue())
          .isBetween(new java.math.BigDecimal("5.00"), new java.math.BigDecimal("900.00"));
      String note = event.path("note").asText();
      if (SyntheticDisputes.TEST_CARDS.stream().anyMatch(note::contains)) {
        withCard++;
      }
      if (note.contains("@")) {
        withEmail++;
        assertThat(note).containsPattern("[a-z]+\\.[a-z]+@example\\.com");
      }
    }
    // Every card number in the stream is a public test number that passes the Luhn check.
    for (String card : SyntheticDisputes.TEST_CARDS) {
      assertThat(luhn(card.replace(" ", ""))).isTrue();
    }
    assertThat(withCard).isBetween(100, 250);
    assertThat(withEmail).isBetween(250, 450);
  }

  @Test
  void replaysRepeatEarlierEventsExactly() {
    List<SyntheticDisputes.Dispute> events = take(new SyntheticDisputes(1, 0.25), 2000);
    Set<SyntheticDisputes.Dispute> distinct = new HashSet<>(events);
    double replayed = 1 - distinct.size() / (double) events.size();
    assertThat(replayed).isBetween(0.18, 0.32);
    assertThat(new HashSet<>(take(new SyntheticDisputes(1, 0), 2000))).hasSize(2000);
    assertThatThrownBy(() -> new SyntheticDisputes(1, 1.5))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static boolean luhn(String digits) {
    int sum = 0;
    boolean twice = false;
    for (int i = digits.length() - 1; i >= 0; i--) {
      int d = digits.charAt(i) - '0';
      if (twice) {
        d *= 2;
        if (d > 9) {
          d -= 9;
        }
      }
      sum += d;
      twice = !twice;
    }
    return sum % 10 == 0;
  }
}
