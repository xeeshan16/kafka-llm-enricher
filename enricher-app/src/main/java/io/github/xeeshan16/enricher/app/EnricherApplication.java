package io.github.xeeshan16.enricher.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Standalone enricher: all behaviour comes from the starter and {@code application.yml}. */
@SpringBootApplication
public class EnricherApplication {

  public static void main(String[] args) {
    SpringApplication.run(EnricherApplication.class, args);
  }
}
