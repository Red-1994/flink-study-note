package com.red.flink.app;


import com.red.flink.source.PostgreSqlSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.util.Map;


/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/5 19:42<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class PostgreSqlTestApp {
    private static Logger logger= LoggerFactory.getLogger(PostgreSqlTestApp.class);


    public static void main(String[] args) throws Exception {

        logger.info(String.format("===========Class:%s Begin main method =============",
                PostgreSqlTestApp.class.getName()));

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //Reader configPath  from  main Args
        ParameterTool argsTool = ParameterTool.fromArgs(args);
        String configPath = argsTool.get("configPath", "/config.properties");
        logger.info(String.format("Main Args configPath=%s", configPath));
        logger.info(String.format("Main Args port=%s", argsTool.get("port")));

        //Reader configPath convert to Properties
        InputStream inputStream = PostgreSqlTestApp.class.getClass().getResourceAsStream(configPath);
        ParameterTool propertiesTool = ParameterTool.fromPropertiesFile(inputStream);
        //Properties convert to Configuration
        Map<String, String> propertiesMap = propertiesTool.toMap();
        Configuration configuration = Configuration.fromMap(propertiesMap);
        logger.info(String.format("Class=%s Configuration=%s", PostgreSqlTestApp.class.getName(), configuration.toString()));
        //Setting Global Configuration
        streamEnv.getConfig().setGlobalJobParameters(configuration);

        //add Source
        logger.info("======================Print to console====================");
        streamEnv.addSource(new PostgreSqlSource("dws", "dwd.dwd_basic_act_hi_taskinst"))
                .setParallelism(2)
                .print();

        streamEnv.execute();

    }

}
