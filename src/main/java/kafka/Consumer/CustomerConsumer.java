package kafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/*
* * This example demonstrates a simple usage of Kafka's consumer api that relies on automatic offset committing.
 * <p>
 * <pre>
 *     Properties props = new Properties();
 *     props.put("bootstrap.servers", "localhost:9092");
 *     props.put("group.id", "test");
 *     props.put("enable.auto.commit", "true");
 *     props.put("auto.commit.interval.ms", "1000");
 *     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 *     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 *     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
 *     consumer.subscribe(Arrays.asList("foo", "bar"));
 *     while (true) {
 *         ConsumerRecords<String, String> records = consumer.poll(100);
 *         for (ConsumerRecord<String, String> record : records)
 *             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
 *     }
 * </pre>
* **/

public class CustomerConsumer {
    public static void main(String[] args) {
        //new KafkaConsumer<>()

        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "192.168.0.2:9092");
        //消费者组id
        props.put("group.id", "test");
        //是否自动提交offset
        props.put("enable.auto.commit", "true");
        //自动提交延时
        props.put("auto.commit.interval.ms", "1000");
        //kv的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //指定topic
        consumer.subscribe(Collections.singleton("topictest")); //消费一个主题
        //consumer.subscribe(Arrays.asList("foo", "bar"));  //消费多个主题
        while (true) {
            //获取数据
            ConsumerRecords<String, String> records = consumer.poll(100);

            //打印信息
            for (ConsumerRecord<String, String> record : records)
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
