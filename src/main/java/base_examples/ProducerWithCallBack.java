package base_examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ProducerWithCallBack {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = ProducerDemo.getStringStringKafkaProducer();
        for (int i = 0; i <= 50; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",
                    "Hello from ProducerWithCallBack [" + i + "]");
            kafkaProducer.send(record, getCallback());
            kafkaProducer.flush();
        }
    }

    public static Callback getCallback() {
        return (recordMetadata, e) -> {
            if (Objects.isNull(e)) {
                logger.info("offset: " + recordMetadata.offset());
                logger.info("topic: " + recordMetadata.topic());
                logger.info("partition: " + recordMetadata.partition());
                logger.info("timestamp: " + recordMetadata.timestamp());
            } else {
                logger.error(e.getMessage());
            }
        };
    }
}
