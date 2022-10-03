package flink.app;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * <b>Flink  故障重启策略 </b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/11 20:03<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class ParameterTest {


    public static void main(String[] args) throws Exception {
        //Construction method Setting parameter

        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> integerDataSource = batchEnv.fromElements(1, 2, 34, 52, 23, 3);

         class MyFilter implements FilterFunction<Integer>{
          private int limit;

          public MyFilter(int limit){
              this.limit=limit;
          }
            @Override
            public boolean filter(Integer value) throws Exception {

                return value>limit;
            }
        }
        integerDataSource.filter(new MyFilter(4))
                .print();

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //固定延迟重启策略
        //允许最大重试次数2，每次重试间隔时间3分钟
        streamEnv.setRestartStrategy(
               RestartStrategies.fixedDelayRestart(2, Time.of(3, TimeUnit.MINUTES))
       );
       //故障率重启策略
        //在5分钟内每30秒重试一次，如果重试次数超过3次，视为重启失败。
        streamEnv.setRestartStrategy(
                RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES),Time.of(30, TimeUnit.SECONDS))
        );

        //没有重启策略
        //作业直接失败，不尝试重启
        streamEnv.setRestartStrategy(
                RestartStrategies.noRestart()
        );
       //后背重启策略
        //使用群集定义的重新启动策略。这对于启用检查点的流式传输程序很有帮助。
        streamEnv.setRestartStrategy(
                RestartStrategies.fallBackRestart()
        );


    }
}
