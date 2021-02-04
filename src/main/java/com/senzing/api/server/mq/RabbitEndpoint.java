package com.senzing.api.server.mq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.senzing.api.services.SzMessage;
import com.senzing.api.services.SzMessageSink;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.senzing.io.IOUtilities.*;

/**
 * Provides a RabbitMQ implementation of {@link SzMessageSink}.
 *
 * The RabbitMQ initialization URL looks like this:
 * <pre>
 *  amqp://{user}:{password}@{host}:{port}/{virtualHost}/{exchange}/{routingKey}[?{prop1}={value1}&{prop2}={value2}]
 * </pre>
 */
public class RabbitEndpoint extends SzAbstractMessagingEndpoint {
  /**
   * The {@link Initiator} for the {@link RabbitEndpoint} class.
   */
  public static final Initiator INITIATOR = new RabbitInitiator();

  /**
   * The pattern for Kafka URL's.
   */
  private static final Pattern URL_PATTERN
      = Pattern.compile(
          "amqp://([^:@]+):([^:@]+)@([^:]+):([0-9]+)"
          + "/([^\\?]*)/([^\\?]+)/([^\\?]+)"
          + "(\\?([^=&]+)=([^=&]+)(&([^=&]+)=([^=&]+))*)?");

  /**
   * The delivery mode to use with RabbitMQ.
   */
  private static final int PERSISTENT_DELIVERY_MODE = 2;

  /**
   * The {@link Channel} to publish the message on.
   */
  private Channel channel;

  /**
   * The exchange for sending the message.
   */
  private String exchange;

  /**
   * The routing key for sending the message.
   */
  private String routingKey;

  /**
   * Constructs with the specified {@link Channel}, exchange and routing key.
   *
   * @param exchange The RabbitMQ exchange for sending messages.
   * @param routingKey The RabbitMQ routing for sending messages.
   * @param channel
   */
  public RabbitEndpoint(Channel channel, String exchange, String routingKey)
  {
    this.exchange   = exchange;
    this.routingKey = routingKey;
    this.channel    = channel;
  }

  @Override
  public void send(SzMessage message) throws Exception {
    // get the message body
    String msgBody = message.getBody();
    byte[] body    = null;
    if (msgBody != null) {
      try {
        body = msgBody.getBytes(UTF_8);

      } catch (UnsupportedEncodingException cannotHappen) {
        throw new IllegalStateException(cannotHappen);
      }
    }

    // get the message properties
    Map<String, String> props = message.getProperties();

    // build the rabbit properties
    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
    builder.deliveryMode(PERSISTENT_DELIVERY_MODE);
    builder.contentEncoding("UTF-8");

    // if we have message properties then add them to the header
    if (props != null && props.size() > 0) {
      Map<String, Object> headers = new LinkedHashMap<>();
      headers.putAll(props);
      builder.headers(headers);
    }

    // create the basic props object
    AMQP.BasicProperties basicProps = builder.build();

    // send the message on the channel
    this.channel.basicPublish(this.exchange, this.routingKey, basicProps, body);
  }

  /**
   * Handles closing the underling {@link Channel} object.
   *
   * @throws Exception If a failure occurs.
   */
  public void close() throws Exception {
    this.channel.close();
  }

  /**
   * Provides an {@link KafkaEndpoint.Initiator} implementation that creates an instance of
   * {@link KafkaEndpoint} from a URL.
   */
  private static class RabbitInitiator
      implements SzMessagingEndpoint.Initiator
  {
    /**
     * Private default constructor.
     */
    private RabbitInitiator() {
      // do nothing
    }

    /**
     * Handles <tt>"amqp:"</tt> URL's to establish an endpoint.
     *
     * @param url The URL for establishing the endpoint.
     */
    public SzMessagingEndpoint establish(String url) {
      final String originalUrl = url;
      if (url == null) return null;
      url = url.trim();
      if (url.length() == 0) return null;
      if (!url.startsWith("amqp:")) return null;
      Matcher matcher = URL_PATTERN.matcher(url);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "The specified RabbitMQ URL does not appear to be properly formed: "
                + originalUrl);
      }
      Map<String,String> props = new LinkedHashMap<>();
      String user         = matcher.group(1);
      String password     = matcher.group(2);
      String host         = matcher.group(3);
      String port         = matcher.group(4);
      String virtualHost  = matcher.group(5);
      String exchange     = matcher.group(6);
      String routingKey   = matcher.group(7);
      String params       = matcher.group(8);

      // normalize the virtual host
      virtualHost = virtualHost.trim();
      virtualHost = (virtualHost.length() == 0) ? "/" : virtualHost;

      // parse the query string into a multi-map
      Map<String, List<String>> paramMap = parseQueryString(params);

      // iterate over the key-value pairs
      paramMap.forEach((key, values) -> {
        if (values.size() > 1) {
          throw new IllegalArgumentException(
              "The specified RabbitMQ URL has multiple values for '" + key
                  + "': " + originalUrl);
        }
        values.forEach(value -> {
          props.put(key, value);
        });
      });

      // create the connection factory
      try {
        ConnectionFactory factory = new ConnectionFactory();

        // set the URI
        factory.setHost(host);
        factory.setPort(Integer.parseInt(port));
        factory.setVirtualHost(virtualHost);
        factory.setUsername(user);
        factory.setPassword(password);

        // add the other properties if specified
        if (paramMap.size() > 0) {
          factory.load(props, "");
        }

        // create the connection
        Connection conn = factory.newConnection();

        // create the channel
        Channel channel = conn.createChannel();

        // create the endpoint
        return new RabbitEndpoint(channel, exchange, routingKey);

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
