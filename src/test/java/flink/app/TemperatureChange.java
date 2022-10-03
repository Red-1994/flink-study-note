package flink.app;

import flink.bean.Temperature;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/9 10:56<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class TemperatureChange {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置时间语义为 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        //封装数据 Temperature

        SingleOutputStreamOperator<Temperature> temperature = dataStream.map(lines -> {
            String[] split = lines.split(",");
            return new Temperature(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        temperature.keyBy("tid")
                .process(new MyTemperatureChangeProcess(5))
                .print("temperature");

        env.execute();
    }

    /**
     * KeyedProcessFunction<Key类型,IN类型,OUT类型>
     *     实现监控在second秒时间范围内，判断温度是否持续上升。
     */
    public static class MyTemperatureChangeProcess extends KeyedProcessFunction<Tuple, Temperature, String> {

        private Integer second;
        public  MyTemperatureChangeProcess(Integer n){
            second=n;
        }
        private ValueState<Double> lastTempState;//上一次温度值
        private ValueState<Long> tsState;//当前定时器时间戳

        //初始化状态

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("lastTemp",Double.class));
            tsState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("ts", Long.class));
        }

        //核心业务处理
        @Override
        public void processElement(Temperature temperature, Context context, Collector<String> collector) throws Exception {
            //取出状态
            Double lastTemp = lastTempState.value();
            Long timerTS = tsState.value();

            //更新 温度状态
            if(lastTemp ==null) {
                lastTemp=temperature.getTvalue();
            }
            lastTempState.update(temperature.getTvalue());
            /**
             * 1.当前温度值大于上一次温度值，且定时器状态为空，注册定时器,更新定时器状态
             * 2.当前温度小于上一次温度值，且定时器不为空，注销定时器,清理定时器状态
             */
            if(temperature.getTvalue()>lastTemp && timerTS==null){
                //获取当前 处理时间+ second*10000
                Long ts= context.timerService().currentProcessingTime()+second*1000L;
                //注册 处理时间 定时器
                context.timerService().registerProcessingTimeTimer(ts);
                //更新定时器状态
                tsState.update(ts);
            }else if(temperature.getTvalue()<lastTemp && timerTS!=null){
                //注销定时器
                context.timerService().deleteProcessingTimeTimer(timerTS);
                //清理定时器状态
                tsState.clear();
            }

        }
        //时间触发器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("timestamp:"+timestamp+" Key: "+ctx.getCurrentKey()
                    +" 温度已经连续"+second+"秒持续上升！");

            //清理定时器状态
            tsState.clear();
        }

    }
}
