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
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello from ProducerDemo");
        //send data - async operation, need flush
        kafkaProducer.send(record);
        // flush and close
        kafkaProducer.close();
    }

    public static KafkaProducer<String, String> getStringStringKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
