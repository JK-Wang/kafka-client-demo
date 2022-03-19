package io.kkk.kafka.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SingleConsumer {
    public static void main(String[] args) {
        String topic = "topic001";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"single-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-single-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Consumer<Object, String> consumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();

        if (partitionInfoList != null) {
            for (PartitionInfo partitionInfo : partitionInfoList) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
            consumer.assign(partitions);

            while (true) {
                ConsumerRecords<Object, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<Object, String> record : records) {
                    System.out.printf("topic = %s, partitions = %s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                consumer.commitAsync();
            }
        }
    }
}
