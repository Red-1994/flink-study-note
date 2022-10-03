package com.red.flink.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.*;
import java.util.Map;

/**
 * <b>自定义的Flink MySQL Source</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/6 17:30<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class PostgreSqlSource extends  RichParallelSourceFunction<Row>   {
    private PreparedStatement ps;
    private Connection connection;
    private Logger logger=LoggerFactory.getLogger(PostgreSqlSource.class);
    private String dbSource;
    private String tableName;

   public PostgreSqlSource(){
       super();
   }
   public PostgreSqlSource(String dbSource, String tableName){
       this.dbSource=dbSource;
       this.tableName=tableName;
   }


    /**
     * Reader JDBC Config
     * Create a connection
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        logger.info(String.format("============Class:%s Begin open method ======================",this.getClass().getName()));
        super.open(parameters);
        //Getting Global Configuration
        ExecutionConfig.GlobalJobParameters globalJobParameters =
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globalConf=(Configuration) globalJobParameters;
        logger.info(String.format("Class=%s Configuration=%s", PostgreSqlSource.class.getName(),globalConf.toString()));

        //Reader JDBC Config
        Map<String, String> parametersMap = globalConf.toMap();
        String driver = parametersMap.get(String.format("jdbc.%s.driver", this.dbSource));
        String url = parametersMap.get(String.format("jdbc.%s.url", this.dbSource));
        String user = parametersMap.get(String.format("jdbc.%s.user", this.dbSource));
        String password = parametersMap.get(String.format("jdbc.%s.password", this.dbSource));

        logger.info(String.format("Create a connection"));
        Class.forName(driver);
        connection=DriverManager.getConnection(url,user,password);

        logger.info("Execute PrepareStatement SQL");
        ps=connection.
                prepareStatement(String.format("select * from %s",tableName));

    }

    @Override
    public void close() throws Exception {
        if(connection!=null){
            connection.close();
        }
        if(ps!=null){
            ps.close();
        }
    }

    /**
     * ResultSet convert  Row
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {

       logger.info(String.format("============Class:%s Begin run method ======================",this.getClass().getName()));
        ResultSet resultSet = ps.executeQuery();
        int columnCount = resultSet.getMetaData().getColumnCount();

        // ResultSet convert to Row
        while (resultSet.next()){
            Row row = new Row(columnCount);
            for (int i = 1; i <=columnCount ; i++) {
                Object object = resultSet.getObject(i);
                //setField Begin pos 0
                row.setField(i-1,object);

            }
            //Collect Result Output
            sourceContext.collect(row);
        }

    }

    @Override
    public void cancel() {

    }

}
