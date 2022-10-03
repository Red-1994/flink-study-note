package com.red.flink.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * <b>同步发生者</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/5 15:01<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class SendSyncProducer {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        //Kafka集群，boker-list
        properties.put("bootstrap.servers","localhost:9092");
        //响应等级
        properties.put("acks","all");
        //重试次数
        properties.put("retries",1);
        //批次大小
        properties.put("batch.size",16484);
        //等待时间
        properties.put("linger.ms",2);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory",33554432);
        //Key ，Value的序列化器
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //发送随机数
        for (int i = 0; i < 100; i++) {
            long round = Math.round(Math.random()*100);
            //发送数据，并且带回调函数
            RecordMetadata kafka = producer.send(new ProducerRecord<String, String>("kafka", "" + i, "" + round),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                System.out.println("Success -> recordMetadata = " + recordMetadata.offset());
                            } else {
                                e.printStackTrace();
                            }
                        }
                    }).get();

            //同步返回 RecordMetadata 信息
            System.out.println("RecordMetadata = " + kafka.toString());

        }
        producer.close();

    }
}
