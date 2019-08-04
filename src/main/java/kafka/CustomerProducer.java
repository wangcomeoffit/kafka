package kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;


/*
* * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
* */

public class CustomerProducer {
    public static void main(String[] args) {
       // new KafkaProducer<>()
        //new ProducerConfig()配置参数解释的源码
        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "192.168.0.2:9092");
        //应对级别
        props.put("acks", "all");
        //重试次数
        props.put("retries",0);
        //批量大小
        props.put("batch.size",16384);
        //提交延时
        props.put("linger.ms",1);
        //整个producer缓存32m
        props.put("buffer.momory",33554432);
        //kv序列化的类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //指定分区
        props.put("partitioner.class","kafka.CustomerPartition");

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 200; i < 300; i++)
            producer.send(new ProducerRecord<String, String>("topictest", String.valueOf(i)), new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println(metadata.partition()+"---"+metadata.offset());
                    }
                    else {
                        System.out.println("发送失败。。。");
                    }
                }
            });


        /*Producer<String, String> producer1 = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));*/
        //关闭资源
        producer.close();
    }
}
