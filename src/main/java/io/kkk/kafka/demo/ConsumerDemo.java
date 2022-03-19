package io.kkk.kafka.demo;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

public class ConsumerDemo {

    Collection<String> topics;
    Consumer<Object, String> consumer;
    String singleTopic;

    ConsumerDemo(String topic, Properties properties) {
        this.topics = new HashSet<>();
        topics.add(topic);
        this.singleTopic = topic;
        consumer = new KafkaConsumer<>(properties);
    }

    public void consume(long interval) {
        consumer.subscribe(topics);
        long start = System.currentTimeMillis();
        while (true) {
            consumer.poll(Duration.ofMillis(interval)).records(singleTopic).forEach(it -> {
                System.out.println("-------\n" +
                        "topic:" + it.topic() +
                        " partition:" + it.partition() +
                        " offset:" + it.offset() +
                        "\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(it.timestamp()));
                System.out.println("content: " + it.value());
            });
            System.out.println("assignment=" + consumer.assignment()); // 获取元数据需要3.5s左右
            if (System.currentTimeMillis() > start + 100000 ) {
                consumer.close();
                break;
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "topic001";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "stream371:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer_demo");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-java-demo");
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


        ConsumerDemo consumer = new ConsumerDemo(topic, properties);
        consumer.consume(1000);
        Thread.sleep(1000);
    }
}
