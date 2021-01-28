package com.senzing.api.server.mq;

import com.senzing.api.services.SzMessage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides an {@link SzMessagingEndpoint} implementation for an Amazon SQS
 * queue.
 *
 * The SQS initialization URL looks like a normal SQS URL but with the
 * <tt>"https://</tt> replaced with <tt>"sqs://"</tt> so it can be recognized
 * as specifically being an SQS url.
 * <pre>
 *  sqs://{hostname}/{accountNumber}/{queueName}
 * </pre>
 */
public class SqsEndpoint extends SzAbstractMessagingEndpoint {
  /**
   * The {@link Initiator} for the {@link RabbitEndpoint} class.
   */
  public static final Initiator INITIATOR = new SqsInitiator();

  /**
   * The pattern for Kafka URL's.
   */
  private static final Pattern URL_PATTERN
      = Pattern.compile("sqs://(([^/]+)/([^\\?]+)/(.+))");


  /**
   * The number of seconds to delay the message (zero).
   */
  private static final int DELAY_SECONDS = 0;

  /**
   * The string message attribute data type.
   */
  private static final String STRING_ATTR_DATA_TYPE = "String";

  /**
   * The {@link SqsClient} to use for sending the requests.
   */
  private SqsClient sqsClient;

  /**
   * The URL for the queue to send to.
   */
  private String queueUrl;

  /**
   * Constructs with the {@link SqsClient} and the queue URL.
   *
   * @param client The {@link SqsClient} to use for connecting.
   * @param queueUrl The URL for the queue.
   */
  public SqsEndpoint(SqsClient client, String queueUrl) {
    this.sqsClient  = client;
    this.queueUrl   = queueUrl;
  }

  /**
   * Provides an implementation for sending on this queue.
   *
   * @param message The message to be sent.
   * @throws Exception Thrown if a failure occurs.
   */
  public void send(SzMessage message) throws Exception {
    // get the message body
    String body = message.getBody();

    // create the message request builder
    SendMessageRequest.Builder builder = SendMessageRequest.builder();
    builder.messageBody(body);
    builder.delaySeconds(DELAY_SECONDS);
    builder.queueUrl(this.queueUrl);

    // check if we have message properties and add them as message attributes
    Map<String, String> props = message.getProperties();
    if (props != null && props.size() > 0) {
      // create the attribute map
      Map<String, MessageAttributeValue> attrMap = new LinkedHashMap<>();

      // iterate over the properties
      props.forEach((key, value) -> {
        // create the builder for the message attribute
        MessageAttributeValue.Builder attrBuilder
            = MessageAttributeValue.builder();

        // build the message attribute
        attrBuilder.dataType(STRING_ATTR_DATA_TYPE);
        attrBuilder.stringValue(value);

        // add the message attribute to the map
        attrMap.put(key, attrBuilder.build());
      });

      // set the message attributes
      builder.messageAttributes(attrMap);
    }

    // build the request
    SendMessageRequest request = builder.build();

    // send the request
    this.sqsClient.sendMessage(request);
  }

  /**
   * Handles closing the underling {@link SqsClient} object.
   *
   * @throws Exception If a failure occurs.
   */
  public void close() throws Exception {
    this.sqsClient.close();
  }

  /**
   * Provides an {@link Initiator} implementation that creates an instance of
   * {@link SqsInitiator} from a URL.
   */
  private static class SqsInitiator implements SzMessagingEndpoint.Initiator {
    /**
     * Default constructor.
     */
    private SqsInitiator() {
      // do nothing
    }

    /**
     * Handles <tt>"kafka:"</tt> URL's to establish
     */
    public SzMessagingEndpoint establish(String originalUrl) {
      String url = originalUrl;
      if (url == null) return null;
      url = url.trim();
      if (url.length() == 0) return null;
      if (!url.startsWith("sqs:")) return null;
      Matcher matcher = URL_PATTERN.matcher(url);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "The specified SQS URL does not appear to be properly formed: "
                + originalUrl);
      }
      String queueUrl = "https://" + matcher.group(1);

      // create the SQS Client
      SqsClient client = SqsClient.create();

      // create the endpoint
      return new SqsEndpoint(client, queueUrl);
    }
  }

}
