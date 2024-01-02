package org.apache.comet;

/** OOM error specific for Comet memory management */
public class CometOutOfMemoryError extends OutOfMemoryError {
  public CometOutOfMemoryError(String msg) {
    super(msg);
  }
}
