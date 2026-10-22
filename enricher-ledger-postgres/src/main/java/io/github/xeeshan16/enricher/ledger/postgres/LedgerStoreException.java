package io.github.xeeshan16.enricher.ledger.postgres;

/** The ledger database could not be reached or rejected a statement. Retrying may help. */
public class LedgerStoreException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public LedgerStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
