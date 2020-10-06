package de.predic8.d_log_retention;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class RetentionDeleteConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(CLIENT_ID_CONFIG, "retention");
        props.put(GROUP_ID_CONFIG, "asd");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe( singletonList("retention_topic2"), new SeekToBeginningRebalanceListener(consumer));

        int num = 0;
        int numOld = -1;
        while (num != numOld) {
            ConsumerRecords<String, String> records = consumer.poll( ofSeconds(1));

            numOld = num;
            num += records.count();

            System.out.println("Gelesene Nachrichten: " + num);

        }

        consumer.close();

    }
}
