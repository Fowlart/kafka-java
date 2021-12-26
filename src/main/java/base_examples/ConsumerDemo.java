package base_examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        // define properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java_app");

        /** After the consumer receives its assignment from the coordinator, it must determine the initial position for
         * each assigned partition. When the group is first created before any messages have been consumed,
         * the position is set according to a configurable offset reset policy (auto.offset.reset). **/
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//earliest/latest/none

        /** First, if you set enable.auto.commit (which is the default), then the consumer will automatically commit
         * offsets periodically at the interval set by auto.commit.interval.ms. The default is 5 seconds. Thus, for durability,
         * disable the automatic commit by setting enable.auto.commit=false and explicitly call one of the commit
         * methods in the consumer code (e.g., commitSync() or commitAsync()) **/
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//earliest/latest/none

        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");

        /** This controls how often the consumer will send heartbeats to the coordinator.
         * It is also the way that the consumer detects when a rebalance is needed, so a lower heartbeat interval will
         * generally mean faster rebalancing. The default setting is three seconds.**/
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        /** Another property that could affect excessive rebalancing is max.poll.interval.ms.
         * This property specifies the maximum time allowed time between calls to the consumers poll method
         * (which returns fetched records based on current partition offset) before the consumer process is assumed to
         * have failed. The default is 300 seconds **/
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000");

        /** Since some messages in the log may be in various states of a transaction, consumers can set the configuration
         * parameter isolation.level to define the types of messages they should receive.
         * By setting isolation.level=read_committed, consumers will receive only non-transactional messages or committed
         * transactional messages. **/
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer
        consumer.subscribe(Collections.singleton("first_topic"));

        //poll for data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> cr : consumerRecords) {
                if ("exit".equalsIgnoreCase(cr.value())) {
                    logger.info(">>> exit signal was received");
                    // System.exit(0);
                }
                logger.info("key: " + cr.key() + "| value: " + cr.value());
            }
        }
    }
}
