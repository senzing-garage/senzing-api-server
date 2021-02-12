package com.senzing.api.server;

import java.util.Set;

class SzApiServerUtilities {
  /**
   * The {@link SzApiServerOption} group for the RabbitMQ info queue options.
   */
  static final String RABBITMQ_INFO_QUEUE_GROUP = "rabbitmq-info";

  /**
   * The {@link SzApiServerOption} group for the Kafka info queue options.
   */
  static final String KAFKA_INFO_QUEUE_GROUP = "kafka-info";

  /**
   * The {@link SzApiServerOption} group for the SQS info queue options.
   */
  static final String SQS_INFO_QUEUE_GROUP = "sqs-info";

  /***
   * The <b>unmodifiable</b> {@link Set} of group names for info queue groups.
   */
  static final Set<String> INFO_QUEUE_GROUPS = Set.of(
      RABBITMQ_INFO_QUEUE_GROUP, KAFKA_INFO_QUEUE_GROUP, SQS_INFO_QUEUE_GROUP);
}
