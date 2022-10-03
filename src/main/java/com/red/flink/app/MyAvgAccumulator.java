package com.red.flink.app;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * <b>累加器</b><br>
 *
 * <p>自定义一个求平均数的累加器</p>
 * <p>  Accumulator<Long,Double>
 *     输入：Long
 *     输出：Double
 * Date: 2022/8/3 16:09<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class MyAvgAccumulator implements Accumulator<Long,Double> {

    private long count; //统计数量
    private double sum;//计算总数

    /**
     * 添加一个元素
     * @param value
     */
    @Override
    public void add(Long value) {
        count++;
        sum+=value;
    }

    /**
     * 获取计算结果值
     * @return
     */
    @Override
    public Double getLocalValue() {
        if(count==0){
            return  0.0;
        }

        return (sum/count);
    }

    /**
     * 重置累加器
     */
    @Override
    public void resetLocal() {
       count=0;
       sum=0.0;
    }

    /**
     * 两个累加器合并操作
     * @param other
     */
    @Override
    public void merge(Accumulator<Long, Double> other) {
      if(other instanceof  MyAvgAccumulator){
         MyAvgAccumulator otherAcc =(MyAvgAccumulator) other;
          this.sum+=otherAcc.sum;
          this.count++;
      }else {
          throw new IllegalArgumentException("The merged accumulator must be MyAvgAccumulator.");
      }
    }

    /**
     * 克隆一个新的累加器并返回
     * @return
     */
    @Override
    public Accumulator<Long, Double> clone() {
        MyAvgAccumulator myAvgAccumulator = new MyAvgAccumulator();
        myAvgAccumulator.count=this.count;
        myAvgAccumulator.sum=this.sum;
        return myAvgAccumulator;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        source.map(new RichMapFunction<Integer, Integer>() {

            private MyAvgAccumulator avgAcc=new MyAvgAccumulator();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("myAvg",avgAcc);
            }

            @Override
            public Integer map(Integer value) throws Exception {
                this.avgAcc.add(value.longValue());
                return value;
            }
        }).setParallelism(1).print();

        JobExecutionResult result = env.execute("MyAvgAccumulatorApp");
        Double myAvg = result.getAccumulatorResult("myAvg");
        System.out.println("Avg Number  = " + myAvg);
    }
}
