package com.senzing.api.services;

/**
 * The message sink for sending messages on the associated queue or topic.
 */
public interface SzMessageSink {
  /**
   * Sends the specified {@link SzMessage} on the associated queue or topic.
   *
   * @param message The {@link SzMessage} to be sent.
   *
   * @throws Exception If a failure occurs in sending the message.
   */
  void send(SzMessage message) throws Exception;
}
