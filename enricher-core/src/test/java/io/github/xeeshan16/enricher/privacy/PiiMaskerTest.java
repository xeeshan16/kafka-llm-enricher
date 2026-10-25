package io.github.xeeshan16.enricher.privacy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class PiiMaskerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final PiiMasker masker = PiiMasker.defaults();

  private static JsonNode json(String text) throws Exception {
    return MAPPER.readTree(text);
  }

  private String mask(String text) {
    return masker.maskText(text);
  }

  @Test
  void masksLuhnValidCardNumbersInAnyGrouping() {
    assertThat(mask("card 4111 1111 1111 1111 was charged")).isEqualTo("card [PAN_1] was charged");
    assertThat(mask("pan=4242-4242-4242-4242;")).isEqualTo("pan=[PAN_1];");
    assertThat(mask("amex 378282246310005")).isEqualTo("amex [PAN_1]");
  }

  @Test
  void leavesNumbersThatAreNotCardsAlone() {
    // Luhn fails, starts with 1 (epoch millis), too short, part of a longer token.
    assertThat(mask("ref 4111111111111112")).isEqualTo("ref 4111111111111112");
    assertThat(mask("at 1729850000000 ms")).isEqualTo("at 1729850000000 ms");
    assertThat(mask("order 424242424242")).isEqualTo("order 424242424242");
    assertThat(mask("id ORD4111111111111111X")).isEqualTo("id ORD4111111111111111X");
    assertThat(mask("amount 349.00 on 2026-10-25")).isEqualTo("amount 349.00 on 2026-10-25");
  }

  @Test
  void masksValidIbansOnlyAndStopsAtTheFollowingWord() {
    assertThat(mask("pay to DE89 3704 0044 0532 0130 00 REF 77"))
        .isEqualTo("pay to [IBAN_1] REF 77");
    assertThat(mask("GB82WEST12345698765432")).isEqualTo("[IBAN_1]");
    // Wrong check digits.
    assertThat(mask("GB00WEST12345698765432")).isEqualTo("GB00WEST12345698765432");
  }

  @Test
  void masksEmailsAndPhoneNumbers() {
    assertThat(mask("reach me at jane.doe+disputes@example.co.uk please"))
        .isEqualTo("reach me at [EMAIL_1] please");
    assertThat(mask("call +44 20 7946 0958 or (555) 123-4567"))
        .isEqualTo("call [PHONE_1] or [PHONE_2]");
    assertThat(mask("version 1.2.3, 12 items, dated 2026-10-25"))
        .isEqualTo("version 1.2.3, 12 items, dated 2026-10-25");
  }

  @Test
  void sameValueGetsTheSamePlaceholderAcrossTheRecord() throws Exception {
    JsonNode event =
        json(
            "{\"card\":\"4111 1111 1111 1111\",\"note\":\"charged twice on 4111-1111-1111-1111,"
                + " also on 4242424242424242\",\"contacts\":[\"a@example.com\",\"b@example.com\","
                + "\"a@example.com\"]}");

    PiiMasker.Masked masked = masker.mask(event);

    assertThat(masked.value().path("card").asText()).isEqualTo("[PAN_1]");
    assertThat(masked.value().path("note").asText())
        .isEqualTo("charged twice on [PAN_1], also on [PAN_2]");
    assertThat(masked.value().path("contacts").toString())
        .isEqualTo("[\"[EMAIL_1]\",\"[EMAIL_2]\",\"[EMAIL_1]\"]");
    assertThat(masked.counts()).containsEntry("PAN", 2).containsEntry("EMAIL", 2);
    assertThat(masked.total()).isEqualTo(4);
    // The input is untouched.
    assertThat(event.path("card").asText()).isEqualTo("4111 1111 1111 1111");
  }

  @Test
  void masksCardNumbersSentAsJsonNumbersAndKeepsOtherNumbers() throws Exception {
    PiiMasker.Masked masked = masker.mask(json("{\"pan\":4111111111111111,\"amount\":12.5}"));

    assertThat(masked.value().path("pan").asText()).isEqualTo("[PAN_1]");
    assertThat(masked.value().path("amount").isNumber()).isTrue();
  }

  @Test
  void fieldPathsAreRedactedWholeIncludingInsideArrays() throws Exception {
    PiiMasker byField =
        new PiiMasker(List.of(PiiDetectors.EMAIL), Set.of("customer.name", "items.owner"));
    JsonNode event =
        json(
            "{\"customer\":{\"name\":\"Jane Doe\",\"email\":\"jane@example.com\"},"
                + "\"items\":[{\"owner\":\"John\",\"sku\":\"A1\"},{\"owner\":null}]}");

    PiiMasker.Masked masked = byField.mask(event);

    assertThat(masked.value().toString())
        .isEqualTo(
            "{\"customer\":{\"name\":\"[REDACTED]\",\"email\":\"[EMAIL_1]\"},"
                + "\"items\":[{\"owner\":\"[REDACTED]\",\"sku\":\"A1\"},{\"owner\":null}]}");
    assertThat(masked.counts()).containsEntry("REDACTED", 2).containsEntry("EMAIL", 1);
  }

  @Test
  void customDetectorsPlugIn() {
    PiiDetector customerId =
        PiiDetectors.regex("CUSTOMER", Pattern.compile("CUST-\\d{6}"), m -> true);
    PiiMasker custom = new PiiMasker(List.of(customerId), Set.of());

    assertThat(custom.maskText("ticket for CUST-004211 and CUST-004211"))
        .isEqualTo("ticket for [CUSTOMER_1] and [CUSTOMER_1]");
  }

  @Test
  void noneMasksNothingAndReturnsTheSameNode() throws Exception {
    JsonNode event = json("{\"card\":\"4111111111111111\"}");
    PiiMasker.Masked masked = PiiMasker.NONE.mask(event);

    assertThat(masked.value()).isSameAs(event);
    assertThat(masked.counts()).isEmpty();
    assertThat(PiiMasker.NONE.enabled()).isFalse();
  }

  @Test
  void namedDetectorsAndInvalidPaths() {
    assertThat(PiiDetectors.named(" pan ")).isSameAs(PiiDetectors.PAN);
    assertThat(PiiDetectors.named("Email")).isSameAs(PiiDetectors.EMAIL);
    assertThatThrownBy(() -> PiiDetectors.named("ssn"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ssn");
    assertThatThrownBy(() -> new PiiMasker(List.of(), Set.of("customer.")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void checksums() {
    assertThat(PiiDetectors.luhn("4111111111111111")).isTrue();
    assertThat(PiiDetectors.luhn("4111111111111112")).isFalse();
    assertThat(PiiDetectors.validIban("DE89370400440532013000")).isTrue();
    assertThat(PiiDetectors.validIban("DE89370400440532013001")).isFalse();
  }
}
