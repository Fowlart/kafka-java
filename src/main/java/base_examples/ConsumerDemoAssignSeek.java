package base_examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(String[] args) {
        String topic = "first_topic";

        // define properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java_app_4");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//earliest/latest/none
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, 10L);

        //poll for data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> cr : consumerRecords) {
                if ("exit".equalsIgnoreCase(cr.value())) {
                    logger.info(">>> exit signal was received");
                    System.exit(0);
                }
                logger.info("offset: " + cr.offset());
                logger.info("key: " + cr.key() + "| value: " + cr.value());
            }
        }
    }
}
