package com.pluralsight.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;

public class KafkaConsumerAssignApp {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);

        TopicPartition consumertopic1partition0 = new TopicPartition("consumertopic1", 0);
        TopicPartition consumertopic2partition0 = new TopicPartition("consumertopic2", 2);

        final List<TopicPartition> topics = asList(consumertopic1partition0, consumertopic2partition0);

        consumer.assign(topics);


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value())
                    );
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
