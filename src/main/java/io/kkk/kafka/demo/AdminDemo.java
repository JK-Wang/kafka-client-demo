package io.kkk.kafka.demo;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.record.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AdminDemo {


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        AdminDemo adminDemo = new AdminDemo();
        adminDemo.initKafkaAdminClient();

        System.out.println(adminDemo.admin.listTopics().listings().get());

//        CountDownLatch latch = new CountDownLatch(1);
//        Callable<KafkaFuture<Void>> futureCallable = () -> adminDemo.admin.deleteTopics(Collections.singleton("topci001")).all().whenComplete((p1, p2) -> latch.countDown());
//        KafkaFuture<Void> kafkaFuture = ThreadPool.submitKafkaAdminClientTask(futureCallable).get();
//        latch.await();
//        System.out.println(kafkaFuture.isCompletedExceptionally());

        // region 查询 Broker 配置
//        DescribeConfigsResult describeConfigsResult = adminDemo.admin.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, "1")));
//        describeConfigsResult.all().get().forEach((configResource, config) -> {
//            System.out.println(configResource);
//            System.out.println(config.get("delete.topic.enable").value());
//        });
        // endregion

//        adminDemo.printACLs();
//        adminDemo.admin.electLeaders(ElectionType.PREFERRED, Collections.singleton(new TopicPartition("topic001", 0)));
//        File file = new File("/Users/jk/tempFile/00000000000000000000.log");
//        admin.printContent(file);
//        admin.dumpLog(file, true);
//        admin.getLogDirs();
    }


    Admin admin;

    private void initKafkaAdminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // region 安全配置
//        properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
//        properties.put(SaslConfigs.SASL_MECHANISM, SaslConfigs.GSSAPI_MECHANISM);
//        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//
//        System.setProperty("java.security.auth.login.config", "/Users/jk/tempFile/auth/jaas.conf");
//        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        // endregion

        this.admin = KafkaAdminClient.create(properties);
    }

    public void getLogDirs() throws ExecutionException, InterruptedException {
//        System.out.println(admin.);
        Collection<Integer> brokers = new HashSet<>();
        brokers.add(1);
        brokers.add(2);
        brokers.add(3);
        DescribeLogDirsResult describeLogDirsResult = admin.describeLogDirs(brokers);
        describeLogDirsResult.all().get().values().forEach(System.out::println);
    }

    public void printContent(File file) throws IOException {
        FileRecords fileRecords = FileRecords.open(file);
        for (FileLogInputStream.FileChannelRecordBatch batch : fileRecords.batches()) {
            for (Record record : batch) {
                System.out.print("| offset:" + record.offset() + "\ttimestamp:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(record.timestamp()));
                ByteBuffer byteBuffer = record.value();
                int length = byteBuffer.limit();
                byte[] dest = new byte[length];
                System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), dest, 0, length);
                System.out.println("\tpayload:" + new String(dest));
            }
        }
    }

    public void dumpLog(File file, boolean printContents) throws IOException {
        long validBytes = 0L;
        long lastOffset = -1L;
        FileRecords fileRecords = FileRecords.open(file);
        for (FileLogInputStream.FileChannelRecordBatch batch : fileRecords.batches()) {
            for (Record record : batch) {
                if (lastOffset == -1) {
                    lastOffset = record.offset();
                } else if (record.offset() != lastOffset + 1) {

                }
                lastOffset = record.offset();

                System.out.println("| offset:" + record.offset() + " timestamp:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(record.timestamp()));
                if (batch.isControlBatch()) {
                    short controlTypeId = ControlRecordType.parseTypeId(record.key());
                    switch (ControlRecordType.fromTypeId(controlTypeId)) {
                        case ABORT:
                        case COMMIT:
                            EndTransactionMarker endTxnMarker = EndTransactionMarker.deserialize(record);
                            System.out.println("endTxnMarker: " + endTxnMarker.controlType() + "coordinatorEpoch: " + endTxnMarker.coordinatorEpoch());
                        default:
                            System.out.println("controlId: " + controlTypeId);
                    }
                } else if (printContents) {
//                    System.out.println(new String(Utils.readBytes(record.value()), StandardCharsets.UTF_8));
                    ByteBuffer byteBuffer = record.value();
                    int length = byteBuffer.limit();
                    byte[] dest = new byte[length];
                    System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), dest, 0, length);
                    System.out.println(new String(dest));
                }
            }
        }
    }

    private void printACLs() {
        try {
            admin.describeAcls(AclBindingFilter.ANY).values().get().forEach(aclBinding -> {
                System.out.println(aclBinding.pattern());
                System.out.println(aclBinding.entry());
                System.out.println();
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
