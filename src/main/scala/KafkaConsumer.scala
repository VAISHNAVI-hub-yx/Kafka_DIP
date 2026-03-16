import java.util.Properties
//Kafka Consumer used to consume messages from the topic. It also reads the password from JCEKS file to connect to the Kafka cluster securely.
//ConsumerRecords is used to store the records fetched from the Kafka topic.
// The consumer subscribes to the specified topic and continuously polls for new messages, printing them to the console and writing them to a file.
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecords}
import scala.jdk.CollectionConverters._
import java.time.Duration
import java.util.Collections
import java.io.{PrintWriter, File}
//import java.time.LocalDateTime

//used to read the password from JCEKS file
import org.apache.hadoop.conf.Configuration
//used to read encrypted passwords from JCEKS file
import org.apache.hadoop.security.alias.CredentialProviderFactory

object KafkaConsumer {
  // Function to read password from JCEKS safely
//  conf : Hadoop Configuration object to access the JCEKS file
//  alias: The alias name of the password in the JCEKS file
  def getPassword(conf: Configuration, alias: String): String = {
//    Fetch the password from the JCEKS file using the provided alias. If the alias is not found, throw an exception.
    val pass = conf.getPassword(alias)
    if (pass == null) {
//      if alias is not found in the JCEKS file, it will return null. In that case, we throw a RuntimeException to indicate the issue.
      throw new RuntimeException("Alias not found in JCEKS: " + alias)
      }
//    convert the password from a character array to a string using mkString, which concatenates the characters into a single string.
    pass.mkString
    }
  def main(args: Array[String]): Unit = {

    val topic = "vi-sap-settlement-detail"
//    port 9093 indicates SSL secured Kafka
    val broker = "kafka-ttc-sapfinance.dev.target.com:9093"
    val groupId = "apprentice_test"

    // JCEKS file path
    val jceksPath =
    "jceks://file///Users/Z00HDDC/IdeaProjects/Kafka/certs/dip_passwords.jceks"

//    creates a Hadoop Configuration object to read the passwords from the JCEKS file.
//    It sets the path to the JCEKS file in the configuration and then calls the getPassword function to retrieve the truststore and keystore passwords.
    val conf = new Configuration()
    conf.set(
    CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
    jceksPath
    )

    // Read passwords from JCEKS
    val truststorePassword = getPassword(conf, "src.truststore.pwd")
    val keystorePassword = getPassword(conf, "src.keystore.pwd")
    // Key password usually same as keystore password
    val keyPassword = keystorePassword


// creates a Properties object to store Kafka configuration
    val props = new Properties()
//    specify kafka broker address
    props.put("bootstrap.servers", broker)
    props.put("group.id", groupId)

//    start consuming from the earliest offset if no previous offset is found for the consumer group.
    //    This ensures that the consumer will read all messages from the beginning of the topic if it's the first time it runs.
    props.put("auto.offset.reset", "earliest")
//    Auto commit is enabled, which means the consumer will automatically commit the offsets of messages it has consumed at regular intervals.
    props.put("enable.auto.commit", "true")

//    Kafka messages are stored as bytes
    //    deserializers to convert the byte data back into strings for both keys and values.
    props.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
    )
//convert message value from byte to string
    props.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // SSL configuration
    props.put("security.protocol", "SSL")

    props.put(
    "ssl.truststore.location",
    "/Users/Z00HDDC/IdeaProjects/Kafka/certs/client.truststore.jks"
    )

    props.put(
    "ssl.keystore.location",
    "/Users/Z00HDDC/IdeaProjects/Kafka/certs/vend-inc-br3-stage.target.com.jks"
    )
//Passwords for the truststore and keystore are read from the JCEKS file to ensure secure handling of sensitive information.
    props.put("ssl.truststore.password", truststorePassword)
    props.put("ssl.keystore.password", keystorePassword)
    props.put("ssl.key.password", keyPassword)
    props.put("ssl.endpoint.identification.algorithm", "") // Disable hostname verification if needed

// creates a KafkaConsumer instance using the configured properties and subscribes to the specified topic. It then enters an infinite loop where it continuously polls for new messages from the Kafka topic.
// For each message received, it prints the partition, offset, key, and value to the console and also writes the message value to a file named "kafka_output.txt".
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(topic))

//creates a PrintWriter to write the consumed messages to a file named "kafka_output.txt". The program then enters an infinite loop where it continuously polls for new messages from the Kafka topic.
// For each message received, it prints the partition, offset, key, and value to the console and also writes the message value to the file.
    val writer = new PrintWriter(new File("kafka_output.txt"))
    println("Kafka Consumer Started...")

    while (true) {
// Fetches records from the Kafka topic with a timeout of 100 milliseconds.
// If there are new messages, it iterates through each record and prints the partition, offset, key, and value to the console.
      val records: ConsumerRecords[String, String] =
      consumer.poll(Duration.ofMillis(100))
//      loop through the fetched records and print their details to the console.
      for (record <- records.asScala) {
        println("Partition: " + record.partition())
        println("Offset: " + record.offset())
        println("Key: " + record.key())
        println("Value: " + record.value())

        writer.println(record.value())

//  flush the writer after writing each message to ensure that the data is written to the file immediately.
        writer.flush()
        }
      }
    }
}