package base_examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = getStringStringKafkaProducer();
        //create a producer record
        // Date theDate = new Date();
        // ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", 0,theDate.getTime(),"to_part_0", "Hello from ProducerDemo");
        //will be sent at the same partition
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "to_part_0", "Hello from ProducerDemo");
        //send data - async operation, need flush
        kafkaProducer.send(record);
        //flush and close
        kafkaProducer.close();
    }

    public static KafkaProducer<String, String> getStringStringKafkaProducer() {
        Properties properties = new Properties();

        // Required properties: BEGIN
        /**You are required to set the bootstrap.servers property so that the producer can find the Kafka cluster.
         * The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this
         * list only impacts the initial hosts used to discover the full set of servers.
         * Since these servers are just used for the initial connection to discover the full cluster membership
         * (which may change dynamically), this list need not contain the full set of
         * servers (you may want more than one, though, in case a server is down). **/
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Required properties: END

        /** To optimize for high durability, it is recommended to set acks=all (equivalent to acks=-1),
         * which means the leader will wait for the full set of in-sync replicas to acknowledge the message
         * and to consider it committed. This provides the strongest available guarantees that the record will not be lost
         * as long as at least one in-sync replica remains alive. **/
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        /**You can use batch.size to control the maximum size in bytes of each message batch. No attempt will be made
         * to batch records larger than this size. Small batch size will make batching less common and may reduce
         * throughput (a batch size of zero will disable batching entirely). A very large batch size may use memory a
         * bit more wastefully as you will always allocate a buffer of the specified batch size in anticipation of
         * additional records.**/
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1024");

        /** To enable retries without reordering, you can set max.in.flight.requests.per.connection to 1 to ensure
         * that only one request can be sent to the broker at a time. **/
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        /**Note additionally that produce requests will be failed before the number of retries has been exhausted if
         * the timeout configured by bellow occurred */
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1000");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500");

        /** Use buffer.memory to limit the total memory that is available to the Java client for collecting unsent
         * messages. This setting should correspond roughly to the total memory the producer will use but is not
         * hardbound since not all memory the producer uses is used for buffering. Some additional memory will be
         * used for compression (if compression is enabled) as well as for maintaining in-flight requests. **/
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024");

        /** Producers can also increase durability by trying to resend messages if any sends fail. The producer automatically
         * tries to resend messages up to the number of times specified by the configuration parameter retries.*/
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(properties);
    }
}
