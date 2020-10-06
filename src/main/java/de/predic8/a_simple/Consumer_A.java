package de.predic8.a_simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class Consumer_A {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, "druck");
        props.put(CLIENT_ID_CONFIG, "a");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe( singletonList("produktion"));

        System.out.println("Consumer A gestartet!");

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

        while(true) {

            ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
            if (records.count() == 0)
                continue;

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition=%d offset= %d, key= %s, value= %s\n",
                record.partition(), 
                record.offset(), 
                record.key(), 
                record.value());

        }
    }
}
