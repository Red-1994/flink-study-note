package flink.app;

import com.red.flink.sink.PostgreSqlSink;
import org.apache.commons.collections4.IteratorUtils;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;

import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <b>测试 CountWindowAll</b><br>
 *
 * <p> 不足 window size 的数据 不会生成新的窗口</p>
 * <p> 示例：  总共 75条数据 ， window size=10
 *            只会生成 7个窗口，最后5条数据不会触发生成窗口
 * Date: 2022/8/1 20:12<br><br>
 *
 * @author 31528
 * @version 1.0
 */

public class CountWindowTest {
    private Logger logger= LoggerFactory.getLogger(PostgreSqlSink.class);

   @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

       integerDataStreamSource.keyBy("");
        DataStreamSink<List<Integer>> print = integerDataStreamSource.countWindowAll(3)

                .process(new ProcessAllWindowFunction<Integer, List<Integer>, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<Integer> elements, Collector<List<Integer>> out) throws Exception {
                        List<Integer> integers = IteratorUtils.toList(elements.iterator());

                        out.collect(integers);
                    }
                })
                .setParallelism(1)
                .print();





        env.execute("CountWindowTest App");
    }
//    class MyTrigger extends Trigger<Integer>
}
