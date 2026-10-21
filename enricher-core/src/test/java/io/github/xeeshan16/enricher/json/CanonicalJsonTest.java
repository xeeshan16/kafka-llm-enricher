package io.github.xeeshan16.enricher.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class CanonicalJsonTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void sortsKeysRecursivelyAndDropsWhitespace() throws Exception {
    String canonical =
        CanonicalJson.write(MAPPER.readTree("{ \"b\": [ {\"z\":1, \"a\":2} ], \"a\": {\"y\":true, \"x\":null} }"));

    assertThat(canonical).isEqualTo("{\"a\":{\"x\":null,\"y\":true},\"b\":[{\"a\":2,\"z\":1}]}");
  }

  @Test
  void hashIgnoresKeyOrderButNotArrayOrderOrValues() throws Exception {
    String h1 = CanonicalJson.sha256(MAPPER.readTree("{\"a\":1,\"b\":[1,2]}"));
    String h2 = CanonicalJson.sha256(MAPPER.readTree("{\"b\":[1,2],\"a\":1}"));
    String h3 = CanonicalJson.sha256(MAPPER.readTree("{\"a\":1,\"b\":[2,1]}"));
    String h4 = CanonicalJson.sha256(MAPPER.readTree("{\"a\":2,\"b\":[1,2]}"));

    assertThat(h1).isEqualTo(h2).isNotEqualTo(h3).isNotEqualTo(h4);
  }

  @Test
  void sha256HexMatchesKnownVector() {
    assertThat(CanonicalJson.sha256Hex("abc"))
        .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  }
}
