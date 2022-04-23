package io.kkk.kafka.demo;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class LowLevelConsumer {
    public static void main(String[] args) throws InterruptedException {
        String topic = "topic001";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer_demo");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-low-level");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // region 安全配置
//        properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
//        properties.put(SaslConfigs.SASL_MECHANISM, SaslConfigs.GSSAPI_MECHANISM);
//        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//
//        System.setProperty("java.security.auth.login.config", "/Users/jk/tempFile/auth/jaas.conf");
//        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        // endregion

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partition : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(topic, partition.partition()));
        }
        consumer.assign(partitions);
        long timestamp = 1753928032L;
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> timestamp)));
        topicPartitionOffsetAndTimestampMap.forEach((tp, offsetAndTimestamp) -> {
            System.out.println("Seeking to " + offsetAndTimestamp.offset());
            consumer.seek(tp, offsetAndTimestamp.offset());
        });


//        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
//            @Override
//            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
//
//            @Override
//            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                System.out.println("Assigned " + partitions);
//                for (TopicPartition tp : partitions) {
//                    OffsetAndMetadata oam = consumer.committed(tp);
//                    if (oam != null) {
//                        System.out.println("Current offset is " + oam.offset());
//                    } else {
//                        System.out.println("No committed offsets");
//                    }
//                    System.out.println("Seeking to " + 2);
//                    consumer.seek(tp, 2);
//                }
//            }
//        });

        while (true) {
            consumer.poll(Duration.ofMillis(1000)).records(topic).forEach(it -> {
                System.out.println("-------\n" +
                        "topic:" + it.topic() +
                        " partition:" + it.partition() +
                        " offset:" + it.offset() +
                        "\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(it.timestamp()));
                System.out.println("key: " + it.key() + ", content: " + it.value());
            });
            System.out.println("assignment=" + consumer.assignment()); // 获取元数据需要3.5s左右
        }
    }
}
