package com.pluralsight.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;

public class KafkaGroupProducerApp {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        final List<String> topics = asList("my-big-topic");


        try {
            for (int i = 0; i < 100; i++) {

                final ProducerRecord<String, String> abcdefg = new ProducerRecord<>("consumertopic2", "abcdefg " + i);

                producer.send(abcdefg);
                System.out.println(String.format("sent abcdef: %d", i));
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            producer.close();
        }
    }
}
