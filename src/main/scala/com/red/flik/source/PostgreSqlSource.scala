package com.red.flik.source

import java.io.InputStream
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util
import java.util.Properties
import  com.esotericsoftware.kryo.serializers.ObjectField
import com.red.flik.util.ConnectionBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.types.Row
/**
 * <b> PostgreSql Source</b><br>
 *
 * <p></p>
 *
 * Date: 2022/7/14 17:02<br><br>
 *
 * @author 31528
 * @version 1.0
 */
class PostgreSqlSource(dbSource:String,tableName:String) extends RichSourceFunction[Row]{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var conn:Connection=null
  var ps:PreparedStatement=null

  /**
   * Create an Connection from  .properties file
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    logger.info(s"=================ClassName:${this.getClass.getName} Beginning Open method:Create an Connection=======================")
    super.open(parameters)
    val globalJobParameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val configMap: util.Map[String, String] = globalJobParameters.toMap
    val configPath:String= configMap get "configPath"
    logger.info(s"=================Load config file:$configPath=================================")
    val pathIn: InputStream = this.getClass.getResourceAsStream(configPath)
    val properties = new Properties()
        properties.load(pathIn)
    conn=ConnectionBuilder.getConnection(properties,dbSource)
    ps=conn.prepareStatement(s"select * from $tableName")
  }

  /**
   * Convert  PreparedStatement to Row
   * @param sourceContext
   */
  override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit ={
    logger.info(s"============ClassName:${this.getClass.getName} Beginning run method: Convert  PreparedStatement to Row=============================")

    val rs: ResultSet = ps.executeQuery()
    val columnCount: Int = rs.getMetaData.getColumnCount
    logger.info("")

    while (rs.next()){
      var row = new Row(columnCount)
       for(i <- 1 to columnCount){
         // setField  pos 0-based
         row.setField(i-1,rs.getArray(i))
       }
      // Output result
      sourceContext.collect(row)

    }

  }

  override def close(): Unit = {
    logger.info(s"============ClassName:${this.getClass.getName} Beginning close method:close connection =============================")

    if (conn!=null) conn.close()
    if (ps!=null) ps.close()
  }

  override def cancel(): Unit = ???
}
