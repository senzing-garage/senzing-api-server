package com.senzing.api.server.mq;

import com.senzing.api.services.SzMessage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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
   * The prefix to use for the initialization properties.
   */
  public static final String PROPERTY_PREFIX = "sqs-";

  /**
   * The property key for the URL.
   */
  public static final String URL_PROPERTY_KEY = PROPERTY_PREFIX + "url";

  /**
   * The <b>unmodifiable</b> {@link Set} of {@link String} property keys for
   * creating an {@link SqsEndpoint}.
   */
  public static final Set<String> PROPERTY_KEYS = Set.of(URL_PROPERTY_KEY);

  /**
   * The {@link Initiator} for the {@link RabbitEndpoint} class.
   */
  public static final Initiator INITIATOR = new SqsInitiator();

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

  @Override
  public void send(SzMessage message, FailureHandler onFailure)
      throws Exception
  {
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

    // send the request -- handling any exceptions
    try {
      this.sqsClient.sendMessage(request);

    } catch (Exception e) {
      // check if we have a failure handler
      if (onFailure != null) {
        // handle the exception
        onFailure.handle(e, message);
      }

      // rethrow
      throw e;
    }
  }

  /**
   * Handles closing the underling {@link SqsClient} object.
   *
   * @throws Exception If a failure occurs.
   */
  public void doClose() throws Exception {
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
     * Handles establishing an SQS endpoint.
     */
    @Override
    public SzMessagingEndpoint establish(Map<String, ?> props,
                                         int            concurrency)
    {
      if (props == null) return null;
      int count = 0;
      for (String key: this.getPropertyKeys()) {
        if (props.containsKey(key)) count++;
      }
      if (count == 0) return null;
      String queueUrl = (String) props.get(URL_PROPERTY_KEY);

      // create the SQS Client
      SqsClient client = SqsClient.create();

      // create the endpoint
      return new SqsEndpoint(client, queueUrl);
    }

    /**
     * Implemented to return {@link #PROPERTY_KEYS}.
     */
    @Override
    public Set<String> getPropertyKeys() {
      return PROPERTY_KEYS;
    }
  }

}
