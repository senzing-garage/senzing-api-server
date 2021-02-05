package com.senzing.api.server.mq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.senzing.api.services.SzMessage;
import com.senzing.api.services.SzMessageSink;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Pipe;
import java.util.*;
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
   * The amount of time to wait for the pool to have an available channel.
   */
  public static final long POOL_WAIT_TIME = 3000L;

  /**
   * The maximum amount of time to wait for a channel to become available.
   */
  public static final long MAX_POOL_WAIT_TIME = 15000L;

  /**
   * Provides a wrapper for a {@link Channel} instance that implements the
   * {@link SzMessageSink} interface.
   */
  private class ChannelSink implements SzMessageSink {
    /**
     * The {@link Channel} instance associated with this sink.
     */
    private Channel channel;

    /**
     * Constructs with the specified {@link Channel} object.
     *
     * @param channel The {@link Channel} instance to construct with.
     */
    public ChannelSink(Channel channel) {
      this.channel = channel;
    }

    /**
     * Implemented to send the specified {@link SzMessage} on the associated
     * {@link Channel}.
     */
    @Override
    public void send(SzMessage message) throws Exception {
      // get the acquired message sink for this thread
      SzMessageSink acquiredSink = RabbitEndpoint.this.getAcquiredSink();

      // check if there is no acquired sink
      if (acquiredSink == null) {
        throw new IllegalStateException(
            "This message sink appears to have already been released and "
            + "can no longer be used.");
      }

      // check if the acquired sink is different
      if (acquiredSink != this) {
        throw new IllegalStateException(
            "The SzMessageSink acquired on this thread is not the same.  It "
            + "appears that this message sink has already been released and "
            + "can no longer be used");
      }

      CONTEXT_SINK.set(this);
      try {
        // send the message
        RabbitEndpoint.this.send(message);

      } finally {
        CONTEXT_SINK.set(null);
      }
    }
  }

  /**
   * The thread-local {@link ChannelSink} to use for calling the
   * {@link #send(SzMessage)} method.
   */
  private static final ThreadLocal<ChannelSink> CONTEXT_SINK
      = new ThreadLocal<>();

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
   * The {@link List} of {@link Channel} instances to pool.
   */
  private List<Channel> channelPool;

  /**
   * The {@link List} of all {@link Channel} instances.
   */
  private List<Channel> allChannels;

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
   * @param channels The {@link List} of {@link Channel} instances to use for
   *                 the channel pool.
   */
  public RabbitEndpoint(List<Channel> channels,
                        String        exchange,
                        String        routingKey)
  {
    this.exchange     = exchange;
    this.routingKey   = routingKey;
    this.allChannels  = new ArrayList<>(channels);
    this.channelPool  = new LinkedList<>();
    this.channelPool.addAll(channels);
  }

  /**
   * Override this method to implement pooling.  The default implementation
   * returns a new {@link SzMessageSink} interface reference to this instance.
   *
   * @return The acquired {@link SzMessageSink}.
   */
  protected SzMessageSink doAcquireMessageSink() {
    synchronized (this.monitor) {
      long startTime = System.currentTimeMillis();
      while (this.channelPool.size() == 0) {
        try {
          // check if we have waited more than the maximum amount of time
          long now = System.currentTimeMillis();
          if (now - startTime > MAX_POOL_WAIT_TIME) {
            throw new IllegalStateException(
                "Waited too long to obtain a pooled channel: "
                    + (now - startTime) + "ms");
          }

          // determine the wait time
          long maxWaitTime = MAX_POOL_WAIT_TIME - (now - startTime);
          long waitTime = (POOL_WAIT_TIME < maxWaitTime)
              ? POOL_WAIT_TIME : maxWaitTime;

          // wait for a channel
          this.monitor.wait(waitTime);

        } catch (InterruptedException ignore) {
          // ignore
        }
      }

      // remove a channel from the pool
      Channel channel = this.channelPool.remove(0);

      // create a message sink with the channel
      return new ChannelSink(channel);
    }
  }

  /**
   * Implemented to return the {@link Channel} tp the pool.
   */
  protected void doReleaseMessageSink(SzMessageSink sink) {
    synchronized (this.monitor) {
      ChannelSink channelSink = (ChannelSink) sink;
      this.channelPool.add(channelSink.channel);
    }
  }

  @Override
  public void send(SzMessage message) throws Exception {
    // get the sink
    boolean acquired = false; // flag to indicate if acquired from the pool

    // check the context sink
    ChannelSink sink = CONTEXT_SINK.get();

    // if no context sink, then check to see if one is already acquired
    if (sink == null) {
      sink = (ChannelSink) this.getAcquiredSink();
    }

    // if none acquired then acquire one from the pool, and mark for release
    if (sink == null) {
      sink = (ChannelSink) this.acquireMessageSink();
      acquired = true;
    }

    try {
      // get the channel
      Channel channel = sink.channel;

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
      channel.basicPublish(this.exchange, this.routingKey, basicProps, body);

    } finally {
      if (acquired) {
        this.releaseMessageSink(sink);
      }
    }
  }

  /**
   * Handles closing the underling {@link Channel} object.
   *
   * @throws Exception If a failure occurs.
   */
  public void doClose() throws Exception {
    synchronized (this.monitor) {
      for (Channel channel: this.allChannels) {
        channel.close();
      }
      this.allChannels.clear();
      this.channelPool.clear();
    }
  }

  /**
   * Provides an {@link KafkaEndpoint.Initiator} implementation that creates
   * an instance of {@link KafkaEndpoint} from a URL.
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
    public SzMessagingEndpoint establish(String url, int concurrency) {
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

        // verify the exchange
        try {
          channel.exchangeDeclarePassive(exchange);
        } catch (IOException e) {
          System.err.println(
              "The specified RabbitMQ does not exist: " + exchange);
          throw e;
        }

        // now create a channel list
        List<Channel> channels = new ArrayList<>(concurrency);
        channels.add(channel);
        for (int index = 0; index < (concurrency - 1); index++) {
          channels.add(conn.createChannel());
        }

        // create the endpoint
        return new RabbitEndpoint(channels, exchange, routingKey);

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
