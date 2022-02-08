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
        for (int i = 0; i <= 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",
                    "Hello from ProducerWithCallBack [" + i + "]");
            kafkaProducer.send(record, getCallback());
            kafkaProducer.flush();
        }
    }

    public static Callback getCallback() {
        return (recordMetadata, e) -> {
            if (Objects.isNull(e)) {
                // recordMetadata - something that will be returned from Kafka, as ???delivery acknowledgment???
                logger.info(
                        " offset: " + recordMetadata.offset() +
                                " topic: " + recordMetadata.topic() +
                                " partition: " + recordMetadata.partition() +
                                " timestamp: " + recordMetadata.timestamp());
            } else {
                logger.error(e.getMessage());
            }
        };
    }
}
