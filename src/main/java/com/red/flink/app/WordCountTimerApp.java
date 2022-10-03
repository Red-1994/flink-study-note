package com.red.flink.app;

import com.red.flink.bean.WordCountPojo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Stack;

/**
 * <b>测试 定时器 统计单词数量</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/8 21:00<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class WordCountTimerApp {


    public static void main(String[] args) throws Exception {
        Logger.getRootLogger().setLevel(Level.ERROR);

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //自定义一个数据源,每2秒输出一个单词
        DataStreamSource<String> source = streamEnv.addSource(new RichSourceFunction<String>() {

            private Stack<String> wordsStack;

            @Override
            public void open(Configuration parameters) throws Exception {
                wordsStack = new Stack<>();
                wordsStack.push("Java");
                wordsStack.push("Scala");
                wordsStack.push("Spark");
                wordsStack.push("Flink");
                wordsStack.push("Flink");
                wordsStack.push("Flink");
                wordsStack.push("Flink");
                wordsStack.push("Java");
                wordsStack.push("Java");
                wordsStack.push("Scala");
                wordsStack.push("Scala");
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {

                while (! wordsStack.empty()) {
                    ctx.collect(wordsStack.pop());
                    Thread.sleep(1000 * 2);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.map(word -> Tuple2.of(word, 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });
        words.keyBy(0)
             .sum(1)
             .print()
             .setParallelism(1);

        words.keyBy(0)
                .process(new KeyByWordFunction(4))
                .print()
                .setParallelism(1);
         words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
             @Override
             public String getKey(Tuple2<String, Integer> value) throws Exception {
                 return value.f0;
             }
         }).process(new KeyByWordStringFunction(4))
                 .print()
                 .setParallelism(1);


        SingleOutputStreamOperator<WordCountPojo> wordPojo = source.map(value -> new WordCountPojo(value, 1));
        wordPojo.keyBy(WordCountPojo::getWord)
                .process( new KeyByWordCountFunction(4))
                .print()
                .setParallelism(1);

        streamEnv.execute("WordCountTimerApp");

    }

    /**
     * KeyedProcessFunction<Key,IN,OUT>
     *  Key 类型是 Tuple,
     *  IN 类型是 Tuple2<String,Integer>
     */

    public static class KeyByWordFunction extends KeyedProcessFunction<Tuple,Tuple2<String,Integer>,Tuple2<String,Integer>> {

        private ValueState<Integer> sumState;//统计单词个数
        private ValueState<Long>  tsState;//定时器的时间戳
        private  Integer second; //控制定时器几秒后触发
        public KeyByWordFunction(Integer n){
            this.second=n;
        }

        /**
         * 初始化
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            sumState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("sum",Integer.class)
            );
            tsState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("ts",Long.class)
            );
        }

        /**
         * 核心业务逻辑
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 累加 sumState
            if(sumState.value()==null){
                sumState.update(0);
            }
            sumState.update(sumState.value()+value.f1);


            /** 如果当前 tsState 为空，
             *  处理时间事件： 就获取当前时间戳，注册一个second 秒后触发的定时器
             */
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            if(tsState.value()==null){
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime+second*1000);
            }
            tsState.update(currentProcessingTime);

        }

        /**
         * 触发定时器的操作
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            //将 (单词,统计个数) 输出
           //getField begin offset 0
            String currentKey = ctx.getCurrentKey().getField(0);
            Integer currentValue = sumState.value();
            out.collect(Tuple2.of(currentKey,currentValue));

            //清理 State
            sumState.clear();
            tsState.clear();

        }

        @Override
        public void close() throws Exception {

        }
    }

    /**
     * KeyedProcessFunction<Key,IN,OUT>
     *  Key 类型是 String,
     *  IN 类型是 Tuple2<String,Integer>
     */
    public static class KeyByWordStringFunction extends KeyedProcessFunction<String,Tuple2<String,Integer>,Tuple2<String,Integer>> {

        private ValueState<Integer> sumState;//统计单词个数
        private ValueState<Long>  tsState;//定时器的时间戳
        private  Integer second; //控制定时器几秒后触发
        public KeyByWordStringFunction(Integer n){
            this.second=n;
        }

        /**
         * 初始化
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            sumState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("sum",Integer.class)
            );
            tsState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("ts",Long.class)
            );
        }

        /**
         * 核心业务逻辑
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 累加 sumState
            if(sumState.value()==null){
                sumState.update(0);
            }
            sumState.update(sumState.value()+value.f1);


            /** 如果当前 tsState 为空，
             *  处理时间事件： 就获取当前时间戳，注册一个second 秒后触发的定时器
             */
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            if(tsState.value()==null){
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime+second*1000);
            }
            tsState.update(currentProcessingTime);

        }

        /**
         * 触发定时器的操作
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            //将 (单词,统计个数) 输出
            //getField begin offset 0
            String currentKey = ctx.getCurrentKey();
            Integer currentValue = sumState.value();
            out.collect(Tuple2.of(currentKey,currentValue));

            //清理 State
            sumState.clear();
            tsState.clear();

        }

        @Override
        public void close() throws Exception {
            sumState.clear();
            tsState.clear();
        }
    }

    /**
     * KeyedProcessFunction<Key,IN,OUT>
     *  Key 类型是 String,
     *  IN 类型是 WordCountPojo
     */
    public static class KeyByWordCountFunction extends KeyedProcessFunction<String,WordCountPojo,Tuple2<String,Integer>> {

        private ValueState<Integer> sumState;//统计单词个数
        private ValueState<Long>  tsState;//定时器的时间戳
        private  Integer second; //控制定时器几秒后触发
        public KeyByWordCountFunction(Integer n){
            this.second=n;
        }

        /**
         * 初始化
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            sumState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("sum",Integer.class)
            );
            tsState=getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("ts",Long.class)
            );
        }

        /**
         * 核心业务逻辑
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(WordCountPojo value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 累加 sumState
            if(sumState.value()==null){
                sumState.update(0);
            }
            sumState.update(sumState.value()+value.getCount());


            /** 如果当前 tsState 为空，
             *  处理时间事件： 就获取当前时间戳，注册一个second 秒后触发的定时器
             */
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            if(tsState.value()==null){
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime+second*1000);
            }
            tsState.update(currentProcessingTime);

        }

        /**
         * 触发定时器的操作
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            //将 (单词,统计个数) 输出
            //getField begin offset 0
            String currentKey = ctx.getCurrentKey();
            Integer currentValue = sumState.value();
            out.collect(Tuple2.of(currentKey,currentValue));

            //清理 State
            sumState.clear();
            tsState.clear();

        }

        @Override
        public void close() throws Exception {
            //TODO No key set. This method should not be called outside of a keyed context.
//            sumState.clear();
//            tsState.clear();
        }
    }

}
