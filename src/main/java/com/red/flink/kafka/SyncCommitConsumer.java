package com.red.flink.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * <b>同步提交 offset 消费者</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/5 16:00<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class SyncCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //Kafka集群，broker-list
        props.put("bootstrap.servers", "localhost:9092");
        //消费者组ID
        props.put("group.id", "group-01");
        //关闭自动提交offset
        props.put("enable.auto.commit", "false");
        //consumer读取数据的策略
        props.put("auto.offset.reset","latest");

        //Key,Value 的序列化器
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅Topic
        consumer.subscribe(Arrays.asList("kafka"));


        //不断从 broker 中 pull 数据

        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(10000);

            consumerRecords.forEach(
                    record->{
                        System.out.printf("Consumer key:%s value:%s \n",record.key(),record.value());
                    }
            );
            //同步提交，阻塞线程
            consumer.commitSync();
        }
    }
}
