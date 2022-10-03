package com.red.flink.sink;

import com.red.flink.source.PostgreSqlSource;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <b>RichSinkFunction<List<Row>> </b><br>
 *
 * <p>接受入参 List<Row>  </p>
 * <p>
 * Date: 2022/7/29 16:38<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class PostgreSqlSink extends RichSinkFunction<List<Row>> {
    private PreparedStatement ps;
    private PreparedStatement insertPs;
    private Connection connection;

    private String insertSql;
    private Logger logger= LoggerFactory.getLogger(PostgreSqlSink.class);
    private String dbSource;
    private String tableName;
    private Map<String,String> colMap;// Map<colName,DataType>
    public PostgreSqlSink(){

    }

    public PostgreSqlSink(String dbSource, String tableName) {
        this.dbSource = dbSource;
        this.tableName = tableName;
    }

    /**
     * Get connection
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
        connection= DriverManager.getConnection(url,user,password);

        //Truncate table
        logger.info(String.format("Execute SQL: truncate table %s",tableName));
        connection.createStatement().execute(
                String.format("truncate table %s",tableName)
        );
        //get table column's size
        ps=connection.prepareStatement(String.format("select * from %s where 1=2",tableName));
        //get insert into sql
        insertSql=getInsertTableSql();
        insertPs = connection.prepareStatement(insertSql);
        connection.setAutoCommit(false);

    }

    /**
     * Close JDBC
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if(insertPs !=null ){
            insertPs.close();
        }
        if (ps != null){
            ps.close();
        }
        if(connection!=null){
            connection.close();
        }
    }

    /**
     *
     * Everyone window, invoke() only execute once
     * @param values
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Row> values, Context context) throws Exception {
        logger.info("============Batch insert to table===============");
        logger.info(String.format("values:%s",values.size()));

        for (Row row:values) {
            for (int i = 0; i <colMap.size() ; i++) {
                insertPs.setObject(i+1,row.getField(i));
            }
            insertPs.addBatch();
             //TODO 为什么，把insert table 逻辑包装成一个方法再调用，每次只能插入一条数据
            //insertTableFromRow(row);
        }
        int[] ints = insertPs.executeBatch();
        logger.info(String.format("insert table batch rows:%s",ints.length));

//        logger.info("============Each row insert to table===============");
//        insertPs.executeUpdate();

    }


    /**
     * row insert into table
      * @param row
     */
 public void insertTableFromRow(Row row){
    // Setting parameter
   try {
       insertPs = connection.prepareStatement(insertSql);
       for (int i = 0; i <colMap.size() ; i++) {
           insertPs.setObject(i+1,row.getField(i));

       }
       insertPs.addBatch();
   }catch (SQLException e){
       logger.error("method insertTableFromRow Exception:",e);
   }


}

    /**
     *  Append insert SQL
     *  eg: INSERT INTO  table (col0,col1,col2..........)
     *                          VALUES(?,?,?,.......)
     * @return
     */
    public String getInsertTableSql() throws  Exception{
        StringBuffer insertBuffer = new StringBuffer("INSERT INTO ");
        insertBuffer.append(tableName).append(" (");
        StringBuffer valuesBuffer = new StringBuffer("VALUES( ");
        ResultSetMetaData metaData=null;
        int columnCount=0;
         metaData = ps.getMetaData();
         columnCount = metaData.getColumnCount();

        colMap=new HashMap<>();

        // begin offset 1
        for (int i = 1; i <=columnCount ; i++) {
            colMap.put(metaData.getColumnName(i),metaData.getColumnTypeName(i));
            insertBuffer.append(metaData.getColumnName(i)).append(",");
            valuesBuffer.append("?,");
        }
        //delete last ,
        insertBuffer.deleteCharAt(insertBuffer.lastIndexOf(","));
        valuesBuffer.deleteCharAt(valuesBuffer.lastIndexOf(","));
        //add )
        insertBuffer.append(")");
        valuesBuffer.append(")");

        StringBuffer preparedSql = insertBuffer.append(valuesBuffer);
        logger.info(String.format("Prepared insert sql:%s",preparedSql));

       return preparedSql.toString();

    }
}
