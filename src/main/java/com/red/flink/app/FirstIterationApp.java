package com.red.flink.app;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/14 15:02<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class FirstIterationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> longDataStreamSource = env.generateSequence(1, 10);

        //创建迭代器
        IterativeStream<Long> iterate = longDataStreamSource.iterate();

        //每次元素 -1
        DataStream<Long> minusOne = iterate.map(v->v-1);

        //如果是奇数继续进入 iterate
        DataStream<Long> left  = minusOne.filter(v -> v % 2 == 1);
        iterate.closeWith(left);
        //如果是偶数就输出
        DataStream<Long> right= minusOne.filter(v -> v % 2 == 0);
        right.print().setParallelism(1);

        env.execute("Iterate App");
    }
}
