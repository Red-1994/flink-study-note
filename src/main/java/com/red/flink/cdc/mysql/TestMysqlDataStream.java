package com.red.flink.cdc.mysql;

import com.red.flink.util.ReaderConfig;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;

import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * <b>ETL MysqlCDC to PostgreSQL</b><br>
 *
 * <p></p>
 * <p>
 * Date: 2022/7/4 16:33<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class TestMysqlDataStream {
    private static Logger logger= LoggerFactory.getLogger( TestMysqlDataStream.class);
    public static void main(String[] args) throws Exception {
        ReaderConfig readerConfig = new ReaderConfig();
        String path = "/localConfig.properties";
         readerConfig.readerProperties(path);
        Properties prop = readerConfig.getProp();



       logger.info("============Create MySqlSource Reader table====================");
        DebeziumSourceFunction<String> srmSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .username(prop.getProperty("jdbc.local_mysql.user"))
                .password(prop.getProperty("jdbc.local_mysql.password"))
                .port(3306)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial()) //Startup Model
                .databaseList("srm")
                .tableList("srm.products")
                .deserializer(new StringDebeziumDeserializationSchema()) //converts SourceReader to  String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        logger.info(" Enable checkpoints");
//        env.setStateBackend( new MemoryStateBackend());
//       //检查点配置  周期性 1秒
//        env.enableCheckpointing(1000);
//        // 检查点模式 精确一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 检查点 超时时间 60秒
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        // 3. 重启策略配置
//        // 固定延迟重启（隔一段时间尝试重启一次）
//        // 尝试次数3次，每次间隔 10秒
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3,
//                10000
//        ));

        logger.info("==========Sink to console=====================");
        env.addSource(srmSource)
                .print().setParallelism(1);
        env.execute("Test Mysql CDC");
    }
}
