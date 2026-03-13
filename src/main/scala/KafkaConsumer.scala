import java.io.File

object KafkaConsumer {

  import java.util.Properties
//  main kafka class use to read messages from kafka topic
  import org.apache.kafka.clients.consumer.KafkaConsumer
//  contains multiple messages fetched from the Kafka topic
  import org.apache.kafka.clients.consumer.ConsumerRecords
  import scala.jdk.CollectionConverters._
  import java.time.Duration
  import java.util.Collections
  import java.io.PrintWriter
  import java.io.File



    def main(args: Array[String]): Unit = {

      val topic = "vi-sap-settlement-detail"

      val props = new Properties()
//kafka server address and port number, group id for the consumer, deserializer for key and value, and auto offset reset policy.
      // These properties are essential for the consumer to connect to the Kafka cluster and consume messages from the specified topic.
      props.put("bootstrap.servers", "kafka-ttc-sapfinance.dev.target.com:9093")
      props.put("group.id", "apprentice_test")

      props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
      )
//Deserializer : As Kafka stores messages as Byte arrays, we need to specify how to convert these bytes back into a usable format. In this case, we are using the StringDeserializer to convert the byte arrays into strings.
      props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
      )

//offset : Each mesasge has a unique number called offset, which represents its position within a partition.
// read messages from the beginning of the topic if there are no committed offsets for the consumer group.
      // This means that if the consumer group has not previously consumed any messages from the topic, it will start consuming from the earliest available message in the topic.
      props.put("auto.offset.reset", "earliest")


//      SSL (Secure Sockets Layer) configuration
      props.put("security.protocol", "SSL")


//      trustore and keystore are used for SSL authentication.
      //      The truststore contains the certificates of trusted entities,
      //      while the keystore contains the client's own certificate and private key. By specifying the locations of these files, the consumer can establish a secure connection to the Kafka cluster.
      props.put(
        "ssl.truststore.location","/Users/Z00HDDC/IdeaProjects/Kafka/certs/client.truststore (1).jks"
      )
      props.put(
        "ssl.keystore.location","/Users/Z00HDDC/IdeaProjects/Kafka/certs/vend-inc-br3-stage.target.com (1).jks"
      )
      props.put("ssl.truststore.password", "/Users/Z00HDDC/IdeaProjects/Kafka/certs/dip_passwords (1).jceks : src.truststore.pwd")
      props.put("ssl.keystore.password", "/Users/Z00HDDC/IdeaProjects/Kafka/certs/dip_passwords (1).jceks : src.keystore.pwd")
//creates a new KafkaConsumer instance using the specified properties.
// This consumer will be used to subscribe to the Kafka topic and consume messages.
      val consumer = new KafkaConsumer[String, String](props)

//      subscribe to the specified Kafka topic using the subscribe method of the KafkaConsumer. The subscribe method takes a list of topics as an argument, and in this case, we are subscribing to a single topic named "vi-sap-settlement-detail".
      consumer.subscribe(Collections.singletonList(topic))

//      create local file named "kafka_output.txt" and a PrintWriter to write the output to that file.
//      This allows us to save the consumed messages from the Kafka topic into a text file for later analysis or reference.
      val writer = new PrintWriter(new File("kafka_output.txt"))

      println("Kafka Consumer is running...")

      while (true) {
//poll is used to fetch messages from the Kafka topic.
// It takes a duration as an argument, which specifies how long the consumer should wait for messages to arrive before returning. In this case, it waits for 100 milliseconds.
// The poll method returns a ConsumerRecords object, which contains all the messages fetched from the topic during that poll.
        val records: ConsumerRecords[String, String] =
        consumer.poll(Duration.ofMillis(100))

//        loop through the fetched records and print out the partition, offset, key, and value of each message.
        for (record <- records.asScala) {


//kafka topic is divided into partitions, and each message within a partition has a unique offset.
          // the partition and offset information can be useful for tracking the position of the consumer in the topic and for debugging purposes.
          println("Partition: " + record.partition())
          println("Offset: " + record.offset())
//          Key identifies the message and can be used for partitioning and routing messages within Kafka.
          // The value is the actual content of the message that was produced to the topic.
          println("Key: " + record.key())
          println("Value: " + record.value())
          println("--------------------------------")


          writer.println("Partition: " + record.partition())
          writer.println("Offset: " + record.offset())
          writer.println("Key: " + record.key())
          writer.println("Value: " + record.value())
          writer.println("--------------------------------")
//          used to ensure that all buffered data is written to the file. This is important because the PrintWriter may buffer the output for performance reasons, and calling flush() ensures that all the buffered data is actually written to the file immediately.
          writer.flush()
        }

      }
    }
  }
