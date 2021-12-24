package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallBack {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = ProducerDemo.getStringStringKafkaProducer();
        for (int i = 0; i <= 50; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","key "+i, "Hello from ProducerWithCallBack [" + i + "]");
            kafkaProducer.send(record, (recordMetadata, e) -> {
                logger.info("offset: " + recordMetadata.offset());
                logger.info("topic: " + recordMetadata.topic());
                logger.info("partition: " + recordMetadata.partition());
                logger.info("timestamp: " + recordMetadata.timestamp());
            });
        }
        // flush and close
        kafkaProducer.close();
    }
}
