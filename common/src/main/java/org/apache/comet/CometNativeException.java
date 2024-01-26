package org.apache.comet;

/** Parent class for all exceptions thrown from Comet native side. */
public class CometNativeException extends CometRuntimeException {
  public CometNativeException(String message) {
    super(message);
  }
}
