package my_weather;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ComfluentKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ComfluentKafkaConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-lq8gm.westeurope.azure.confluent.cloud:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"FQI6YRXCG7UUYBWE\" password=\"4++vqv8UdQpYzM2RAJ83rBQay8J5ea9lztqrB9XGtgIY9uX19z7izaFdoRE4LCob\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("client.dns.lookup", "use_all_dns_ips");
        properties.setProperty("session.timeout.ms", "45000");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_0");

        // properties.setProperty("schema.registry.url", "");
        // properties.setProperty("basic.auth.credentials.source", "USER_INFO");
        // properties.setProperty("basic.auth.user.info", "");

        KafkaConsumer<String,  String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("orders"));


        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> cr : consumerRecords) {
                logger.info("key: " + cr.key() + "| value: " + cr.value());
            }
        }

    }
}
