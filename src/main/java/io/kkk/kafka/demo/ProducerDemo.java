package io.kkk.kafka.demo;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Properties;

public class ProducerDemo {

    String topic;
    Producer<Object, String> producer;

    ProducerDemo(String topic, Properties properties) {
        this.topic = topic;
        producer = new KafkaProducer<>(properties);
    }

    /**
     * 发送消息
     * @param n 发送n条消息
     * @param callback 是否打印回调结果
     */
    public void send(int n, boolean callback) {
        for (int i = 0; i < n; i++) {
            ProducerRecord<Object, String> record = new ProducerRecord<>(topic, "data"+i);
            if (callback) {
                producer.send(record, (recordMetadata, e) -> {
                    if (e != null) e.printStackTrace();
                    System.out.println("-------\n" +
                            "topic:" + recordMetadata.topic() +
                            " partition:" + recordMetadata.partition() +
                            " offset:" + recordMetadata.offset() + "\n" +
                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordMetadata.timestamp()));
                });
            } else {
                producer.send(record);
            }
        }
        producer.flush();
    }

    /**
     * 持续发送
     * @param interval 发送间隔
     */
    public void send(long interval) {
        long num = 0;
        long start = System.currentTimeMillis();
        while (true) {
            ProducerRecord<Object, String> record = new ProducerRecord<>(topic, "kafka lag test");
            producer.send(record);
            producer.flush();
            num++;
            if (System.currentTimeMillis() - start > 30000) {
                System.out.println("already send: " + num);
                start += 30000;
            }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        String topic = "topic_in";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "stream371:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer_demo");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // region 安全配置
//        properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
//        properties.put(SaslConfigs.SASL_MECHANISM, SaslConfigs.GSSAPI_MECHANISM);
//        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//
//        System.setProperty("java.security.auth.login.config", "/Users/jk/Downloads/jaas.conf");
//        System.setProperty("java.security.krb5.conf", "/Users/jk/Downloads/krb5.conf");
//        System.setProperty("sun.security.krb5.debug","true");
        // endregion

        ProducerDemo producer = new ProducerDemo(topic, properties);
//        producer.send(10, true);
        producer.send(100);
    }
}
