package com.red.flink.app;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * <b>自定义 聚合函数</b><br>
 *
 * <p>计算 N个整数的平均值</p>
 * <p>
 * Date: 2022/8/3 16:55<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class MyAvgAggregator implements AggregateFunction<Long,MyAvgAccumulator,Double> {

    /**
     * 创建一个累加器
     * @return
     */
    @Override
    public MyAvgAccumulator createAccumulator() {
        return new MyAvgAccumulator();
    }

    /**
     * 将value 添加到累加器中
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public MyAvgAccumulator add(Long value, MyAvgAccumulator accumulator) {
          accumulator.add(value);
        return accumulator;
    }

    /**
     * 返回计算结果
     * @param accumulator
     * @return
     */
    @Override
    public Double getResult(MyAvgAccumulator accumulator) {
        return accumulator.getLocalValue();
    }

    /**
     * 两个累加器的合并操作
     * @param a
     * @param b
     * @return
     */
    @Override
    public MyAvgAccumulator merge(MyAvgAccumulator a, MyAvgAccumulator b) {
        a.merge(b);
        return a;
    }

    public static void main(String[] args) throws Exception{
        Logger.getRootLogger().setLevel(Level.ERROR);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 12, 12, 35, 5, 6, 77, 81, 9);
        SingleOutputStreamOperator<Long> mapLong = source.map(
                value -> value.longValue()
      );

        mapLong.countWindowAll(3)
                .aggregate(new MyAvgAggregator())
                .print();

        env.execute("MyAvgAggregator App");
    }
}
