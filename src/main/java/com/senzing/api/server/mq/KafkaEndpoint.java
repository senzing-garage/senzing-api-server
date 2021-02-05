package com.senzing.api.server.mq;

import com.senzing.api.services.SzMessage;
import com.senzing.api.services.SzMessageSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.senzing.io.IOUtilities.UTF_8;

/**
 * Provides a Kafka implementation of {@link SzMessageSink}.
 *
 * The Kafka initialization URL looks like this:
 * <pre>
 *  kafka://{host}:{port}/{topic}[?{prop1}={value1}&{prop2}={value2}]
 * </pre>
 *
 * The optional query-string properties are used to initialize the
 * {@link KafkaProducer}.  If specified, those properties should be
 * single-valued and should omit the <tt>"bootstrap.servers"</tt> property
 * which is specified as the first part of the URL.
 */
public class KafkaEndpoint extends SzAbstractMessagingEndpoint {
  /**
   * The {@link Initiator} for the {@link KafkaEndpoint} class.
   */
  public static final Initiator INITIATOR = new KafkaInitiator();

  /**
   * The pattern for Kafka URL's.
   */
  private static final Pattern URL_PATTERN
      = Pattern.compile("kafka://([^:]+):([0-9]+)/([^\\?]+)"
                        + "(\\?([^=&]+)=([^=&]+)(&([^=&]+)=([^=&]+))*)?");

  /**
   * The producer for sending the messages.
   */
  private KafkaProducer<String, String> producer = null;

  /**
   * The topic on which to publish messages.
   */
  private String topic = null;

  /**
   * Constructs with the specified topic and {@link KafkaProducer}.
   *
   * @param topic The topic to construct with.
   * @param producer The producer to construct with.
   */
  public KafkaEndpoint(String topic, KafkaProducer<String, String> producer)
  {
    this.topic = topic;
    this.producer = producer;
  }

  @Override
  public void send(SzMessage message) throws Exception {
    // create the record to send
    ProducerRecord<String, String> record
        = new ProducerRecord<>(this.topic, message.getBody());

    // add any message properties as headers if they exist
    Map<String, String> props = message.getProperties();
    if (props == null && props.size() > 0) {
      props.forEach((key, value)-> {
        try {
          record.headers().add(key, value.getBytes(UTF_8));

        } catch (UnsupportedEncodingException cannotHappen) {
          throw new IllegalStateException(cannotHappen);
        }
      });
    }

    // send the record
    this.producer.send(record);
  }

  /**
   * Handles closing the underling {@link KafkaProducer} object.
   *
   * @throws Exception If a failure occurs.
   */
  protected void doClose() throws Exception {
    this.producer.close();
  }

  /**
   * Provides an {@link Initiator} implementation that creates an instance of
   * {@link KafkaEndpoint} from a URL.
   */
  private static class KafkaInitiator implements SzMessagingEndpoint.Initiator {
    /**
     * Default constructor.
     */
    private KafkaInitiator() {
      // do nothing
    }

    /**
     * Handles <tt>"kafka:"</tt> URL's to establish
     */
    @Override
    public SzMessagingEndpoint establish(String originalUrl, int concurrency) {
      String url = originalUrl;
      if (url == null) return null;
      url = url.trim();
      if (url.length() == 0) return null;
      if (!url.startsWith("kafka:")) return null;
      Matcher matcher = URL_PATTERN.matcher(url);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "The specified Kafka URL does not appear to be properly formed: "
            + originalUrl);
      }
      Properties  props   = new Properties();
      String      host    = matcher.group(1);
      String      port    = matcher.group(2);
      String      topic   = matcher.group(3);
      String      params  = matcher.group(4);

      props.put("bootstrap.servers", host + ":" + port);

      // parse the query string into a multi-map
      Map<String, List<String>> paramMap = parseQueryString(params);

      // iterate over the key-value pairs
      paramMap.forEach((key, values) -> {
        if (values.size() > 1) {
          throw new IllegalArgumentException(
              "The specified Kafka URL has multiple values for '" + key + "': "
              + originalUrl);
        }
        values.forEach(value -> {
          props.put(key, value);
        });
      });

      // create the producer
      KafkaProducer<String,String> producer = new KafkaProducer<>(props);
      return new KafkaEndpoint(topic, producer);
    }
  }
}
