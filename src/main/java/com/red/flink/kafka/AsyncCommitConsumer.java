package com.red.flink.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.util.Callback;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * <b>异步提交 offset 消费者</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/5 15:53<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class AsyncCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //Kafka集群，broker-list
        props.put("bootstrap.servers", "localhost:9092");
        //消费者组ID
        props.put("group.id", "group-01");
        //关闭自动提交offset
        props.put("enable.auto.commit", "false");

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
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            consumerRecords.forEach(
                    record->{
                        System.out.printf("Consumer key:%s value:%s \n",record.key(),record.value());
                    }
            );
             //如果返回异常,就输出
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e != null){
                     System.err.printf("Commit offset exception Value:%s",map.values());
                    }
                }
            });
        }
    }
}
