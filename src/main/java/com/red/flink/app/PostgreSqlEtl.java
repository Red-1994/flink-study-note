package com.red.flink.app;


import com.red.flink.sink.PostgreSqlSink;
import com.red.flink.sink.PostgreSqlSinkSingle;
import com.red.flink.source.PostgreSqlSource;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.function.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import org.apache.flink.table.expressions.E;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <b>测试自定义 Source 和 Sink </b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/1 10:35<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class PostgreSqlEtl {
    private static Logger logger= LoggerFactory.getLogger(PostgreSqlEtl.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //Getting main parameter
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("configPath", "/config.properties");
        int batchCount = parameterTool.getInt("batchCount", 1000);
        //Getting Resource properties file
        InputStream resourceAsStream = PostgreSqlEtl.class.getResourceAsStream(configPath);
        ParameterTool propertiesTool = ParameterTool.fromPropertiesFile(resourceAsStream);
        // Setting configuration
        Configuration configuration = Configuration.fromMap(propertiesTool.toMap());
        configuration.setInteger("batchCount",batchCount);
        streamEnv.getConfig().setGlobalJobParameters(configuration);


        logger.info("Begin add Source and Sink to ETL");
        DataStreamSource<Row> dwsSource = streamEnv.addSource(new PostgreSqlSource("dws", "dwd.dwd_quality_sqam_claim_form_header"))
                .setParallelism(1);

        dwsSource.addSink(new PostgreSqlSinkSingle("dws","dwd.dwd_quality_sqam_claim_form_header_test"))
                .setParallelism(1);
        /**
         * 这种方法只能插入 1000*n 条数据，最后不到 1000的数据不会插入，会漏掉
         *   因为 countWindowAll 只有当当前窗口数据 count=1000 才会触发创建窗口动作。
         *   示例： 7280 条数据，只会插入7000条数据，它只会生产 7个窗口，最后280条数据不会生成窗口
         */

        dwsSource.countWindowAll(1000)
                .process(new ProcessAllWindowFunction<Row, List<Row>, GlobalWindow>() {
                    private int batch;
                    @Override
                    public void process(Context context, java.lang.Iterable<Row> elements, Collector<List<Row>> out) throws Exception {
                        List<Row> rows = IteratorUtils.toList(elements.iterator());
                        batch=rows.size();
                        if(rows.size()>0){
                            out.collect(rows);
                        }
                    }
                })
                .addSink(new PostgreSqlSink("dws", "dwd.dwd_quality_sqam_claim_form_header_test"))
                .setParallelism(1);

        streamEnv.execute("PostgreSqlEtl App");

    }
}
