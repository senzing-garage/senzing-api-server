package com.senzing.api.server.mq;

import com.senzing.api.services.SzMessage;
import com.senzing.api.services.SzMessageSink;

/**
 * Represents an established connection to a messaging endpoint.
 */
public interface SzMessagingEndpoint extends SzMessageSink, AutoCloseable {
  /**
   * An interface for creating endpoint instances from a URL.
   */
  interface Initiator {
    /**
     * Given the specified {@link String} URL this method will create an
     * instance of {@link SzMessagingEndpoint} if the specified URL is in the
     * format expected for this builder.  If the specified URL format is not
     * handled by this instance then <tt>null</tt> is returned.
     *
     * @param url The URL to use for establishing the endpoint conenction.
     *
     * @return The {@link SzMessagingEndpoint} created for the specified URL,
     *         or <tt>null</tt> if the format of the specified URL is not
     *         handled by this instance.
     */
    SzMessagingEndpoint establish(String url);
  }

  /**
   * Returns an {@link SzMessageSink} interface to this endpoint that lacks
   * access to the other functionality of the endpoint (e.g.: {@link #close()}).
   *
   * @return An {@link SzMessageSink} interace to this instance.
   */
  default SzMessageSink asMessageSink() {
    return (SzMessagingEndpoint.this::send);
  }

  /**
   * Closes the endpoint.
   *
   * @throws Exception If a failure occurs.
   */
  void close() throws Exception;
}
