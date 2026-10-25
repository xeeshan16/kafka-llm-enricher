package io.github.xeeshan16.enricher.llm.http;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/** A local HTTP server that records requests and plays back scripted responses. */
final class StubHttpServer implements AutoCloseable {

  /** A scripted response. */
  record Reply(int status, String body, Map<String, String> headers, Duration delay) {
    static Reply json(int status, String body) {
      return new Reply(status, body, Map.of(), Duration.ZERO);
    }

    Reply withHeader(String name, String value) {
      Map<String, String> all = new java.util.HashMap<>(headers);
      all.put(name, value);
      return new Reply(status, body, all, delay);
    }

    Reply after(Duration wait) {
      return new Reply(status, body, headers, wait);
    }
  }

  /** A received request. */
  record Received(
      String method, String path, String query, Map<String, String> headers, String body) {
    String header(String name) {
      return headers.get(name.toLowerCase(java.util.Locale.ROOT));
    }
  }

  private final HttpServer server;
  private final ConcurrentLinkedQueue<Reply> replies = new ConcurrentLinkedQueue<>();
  final List<Received> received = new CopyOnWriteArrayList<>();

  StubHttpServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try (InputStream in = exchange.getRequestBody()) {
            Map<String, String> headers = new java.util.HashMap<>();
            exchange
                .getRequestHeaders()
                .forEach((k, v) -> headers.put(k.toLowerCase(java.util.Locale.ROOT), v.get(0)));
            received.add(
                new Received(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getRawQuery(),
                    headers,
                    new String(in.readAllBytes(), StandardCharsets.UTF_8)));
          }
          Reply reply = replies.poll();
          if (reply == null) {
            reply = Reply.json(500, "{\"error\":{\"message\":\"no scripted reply\"}}");
          }
          if (!reply.delay().isZero()) {
            try {
              Thread.sleep(reply.delay().toMillis());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          reply.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(reply.status(), bytes.length == 0 ? -1 : bytes.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
    server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    server.start();
  }

  StubHttpServer reply(Reply reply) {
    replies.add(reply);
    return this;
  }

  String baseUrl() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  Received last() {
    return received.get(received.size() - 1);
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
