package com.red.flink.sink;


import com.red.flink.source.PostgreSqlSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
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
 * <b>RichSinkFunction<Row></b><br>
 *
 * <p> 接受参数 Row</p>
 * <p>
 * Date: 2022/8/2 19:56<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class PostgreSqlSinkSingle extends RichSinkFunction<Row>  {
    private Logger logger= LoggerFactory.getLogger(PostgreSqlSink.class);
    private PreparedStatement ps;
    private PreparedStatement insertPs;
    private Connection connection;
    private String insertSql;
    private String dbSource;
    private String tableName;
    private int batchCount;
    private int batchAcc=0;
    private IntCounter batchCounterAcc=new IntCounter();

    private Map<String,String> colMap;// Map<colName,DataType>
    public PostgreSqlSinkSingle(){

    }

    public PostgreSqlSinkSingle(String dbSource, String tableName) {
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

        //Reader batchCount
        batchCount= globalConf.getInteger("batchCount",1000);
        logger.info(String.format("Inset into table batchCount:%s",batchCount));

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
        //Register IntCounter
        getRuntimeContext().addAccumulator("batchAcc",batchCounterAcc);
        connection.setAutoCommit(false);

    }

    /**
     * Close JDBC
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if(batchAcc>0){
            int[] ints = insertPs.executeBatch();
            logger.info(String.format("insert table batch rows:%s",ints.length));
        }

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
    public void invoke(Row values, Context context) throws Exception {
        //TODO 暂时不支持多并行度插入数据
            for (int i = 0; i <colMap.size() ; i++) {
                insertPs.setObject(i+1,values.getField(i));
            }
            insertPs.addBatch();

            batchCounterAcc.add(1);
            batchAcc++;

            //insertTableFromRow(row);
        if(batchCounterAcc.getLocalValuePrimitive()>=batchCount){
            int[] ints = insertPs.executeBatch();
            logger.info(String.format("insert table batch rows:%s",ints.length));
            batchAcc=0;
            batchCounterAcc.resetLocal();
        }


//        logger.info("============Each row insert to table===============");
//        insertPs.executeUpdate();

    }


    /**
     * row insert into table
     * @param row
     */
    public void insertTableFromRow(org.apache.flink.types.Row row){
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
