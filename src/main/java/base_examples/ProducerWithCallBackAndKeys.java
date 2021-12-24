package base_examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ProducerWithCallBackAndKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallBackAndKeys.class);

    /**
     * All partitions will be same each lunch:
     * key: id_0 - partition: 1
     * key: id_1 - partition: 0
     * key: id_2 - partition: 2
     * key: id_3 - partition: 0
     * key: id_4 - partition: 2
     * key: id_5 - partition: 2
     * key: id_6 - partition: 0
     * key: id_7 - partition: 2
     **/

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> kafkaProducer = ProducerDemo.getStringStringKafkaProducer();
        for (int i = 0; i <= 10; i++) {
            String key = "id_" + i;
            logger.info("key: " + key);
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key,
                    "Hello from ProducerWithCallBackAndKeys [" + i + "]");
            kafkaProducer.send(record, (recordMetadata, e) -> {
                logger.info("partition: " + recordMetadata.partition());
            }).get();
            kafkaProducer.flush();
        }
    }
}
