package io.github.xeeshan16.enricher.privacy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The built-in detectors: card numbers, IBANs, email addresses and phone numbers.
 *
 * <p>Card numbers and IBANs are checked (issuer range and Luhn, ISO 7064 mod 97) so that order ids,
 * amounts and timestamps are not masked. Phone numbers are only matched in international form
 * ({@code +} and 8 to 15 digits) or in the North American {@code 555-123-4567} shape, which keeps
 * dates and plain numeric ids out.
 */
public final class PiiDetectors {

  /**
   * Primary account numbers: 13 to 19 digits, optionally grouped by spaces or dashes, starting with
   * 2 to 6 (the ranges card networks issue from) and passing the Luhn check. The leading-digit rule
   * keeps 13-digit epoch-millisecond timestamps, which start with 1, from being masked.
   */
  public static final PiiDetector PAN =
      regex(
          "PAN",
          Pattern.compile("(?<![\\d\\w])[2-6](?:[ -]?\\d){12,18}(?![\\d\\w])"),
          m -> luhn(digits(m)));

  /** International bank account numbers, compact or in groups of four. */
  public static final PiiDetector IBAN = new IbanDetector();

  /** Email addresses. */
  public static final PiiDetector EMAIL =
      regex(
          "EMAIL",
          Pattern.compile(
              "(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}"),
          m -> true);

  /** Phone numbers in international ({@code +44 20 7946 0958}) or North American form. */
  public static final PiiDetector PHONE =
      regex(
          "PHONE",
          Pattern.compile(
              "(?<![\\w+])(?:\\+\\d[\\d ().-]{6,20}\\d|\\(?\\d{3}\\)?[ .-]\\d{3}["
                  + " .-]\\d{4})(?![\\w])"),
          m -> {
            int n = digits(m).length();
            return n >= 8 && n <= 15;
          });

  private static final Map<String, PiiDetector> BY_NAME =
      Map.of("PAN", PAN, "IBAN", IBAN, "EMAIL", EMAIL, "PHONE", PHONE);

  private PiiDetectors() {}

  /** Every built-in detector, in the order they run. */
  public static List<PiiDetector> all() {
    return List.of(PAN, IBAN, EMAIL, PHONE);
  }

  /** A built-in detector by name ({@code pan}, {@code iban}, {@code email}, {@code phone}). */
  public static PiiDetector named(String name) {
    PiiDetector detector = BY_NAME.get(name.strip().toUpperCase(Locale.ROOT));
    if (detector == null) {
      throw new IllegalArgumentException(
          "unknown PII type '" + name + "'; use pan, iban, email or phone");
    }
    return detector;
  }

  /** A detector from a regular expression and a check on each match. */
  public static PiiDetector regex(String label, Pattern pattern, Predicate<String> accept) {
    return new PiiDetector() {
      @Override
      public String label() {
        return label;
      }

      @Override
      public List<Span> find(String text) {
        List<Span> spans = new ArrayList<>();
        Matcher m = pattern.matcher(text);
        while (m.find()) {
          if (accept.test(m.group())) {
            spans.add(new Span(m.start(), m.end()));
          }
        }
        return spans;
      }

      @Override
      public String toString() {
        return label;
      }
    };
  }

  /**
   * IBANs written in groups can run into an upper-case word that follows them ({@code DE89 3704
   * 0044 0532 0130 00 REF}); when the whole match fails the checksum, shorter prefixes that end at
   * a group boundary are tried.
   */
  private static final class IbanDetector implements PiiDetector {
    private static final Pattern CANDIDATE =
        Pattern.compile("(?<![A-Za-z0-9])[A-Z]{2}\\d{2}(?: ?[A-Z0-9]){11,30}");

    @Override
    public String label() {
      return "IBAN";
    }

    @Override
    public List<Span> find(String text) {
      List<Span> spans = new ArrayList<>();
      Matcher m = CANDIDATE.matcher(text);
      int from = 0;
      while (from < text.length() && m.find(from)) {
        int end = m.end();
        Span found = null;
        while (end > m.start()) {
          boolean boundary = end == text.length() || !Character.isLetterOrDigit(text.charAt(end));
          if (boundary && validIban(text.substring(m.start(), end))) {
            found = new Span(m.start(), end);
            break;
          }
          int space = text.lastIndexOf(' ', end - 1);
          if (space <= m.start()) {
            break;
          }
          end = space;
        }
        if (found != null) {
          spans.add(found);
          from = found.end();
        } else {
          from = m.start() + 1;
        }
      }
      return spans;
    }

    @Override
    public String toString() {
      return "IBAN";
    }
  }

  static String digits(String s) {
    StringBuilder b = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c >= '0' && c <= '9') {
        b.append(c);
      }
    }
    return b.toString();
  }

  /** Luhn checksum over a digit string. */
  static boolean luhn(String digits) {
    if (digits.length() < 13 || digits.length() > 19) {
      return false;
    }
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

  /** ISO 13616 IBAN check: length 15 to 34 and mod 97 of the rearranged number equal to 1. */
  static boolean validIban(String candidate) {
    String iban = candidate.replace(" ", "");
    if (iban.length() < 15 || iban.length() > 34) {
      return false;
    }
    String rearranged = iban.substring(4) + iban.substring(0, 4);
    StringBuilder numeric = new StringBuilder(rearranged.length() * 2);
    for (int i = 0; i < rearranged.length(); i++) {
      char c = rearranged.charAt(i);
      if (Character.isDigit(c)) {
        numeric.append(c);
      } else if (c >= 'A' && c <= 'Z') {
        numeric.append(c - 'A' + 10);
      } else {
        return false;
      }
    }
    return new BigInteger(numeric.toString()).mod(BigInteger.valueOf(97)).intValue() == 1;
  }
}
