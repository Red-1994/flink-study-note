package com.red.flink.app;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;


/**
 * <b>计数器</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/3 15:22<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class MyCounterApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.fromElements("A", "B", "C", "D", "E");
        dataSource.map(
                new RichMapFunction<String, String>() {

                    //1.创建计数器
                    private IntCounter intAcc = new IntCounter();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //2.注册计数器
                        getRuntimeContext().addAccumulator("wordCount", intAcc);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        //3.使用计数器
                        this.intAcc.add(1);
                        return value;
                    }
                }
        ).setParallelism(1).print();

        //4.获取累加器
        JobExecutionResult result = env.execute();
        int accumulatorResult = result.getAccumulatorResult("wordCount");
        System.out.println(String.format(" accumulatorResult=%s",accumulatorResult));

    }
}
