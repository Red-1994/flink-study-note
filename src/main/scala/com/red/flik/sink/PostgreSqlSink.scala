package com.red.flik.sink

import java.io.InputStream
import java.sql.{Connection, PreparedStatement, ResultSetMetaData}
import java.util
import java.util.Properties

import com.red.flik.util.ConnectionBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Map
/**
 * <b>PostgreSql  Sink</b><br>
 *
 * <p></p>
 *
 * Date: 2022/7/15 11:27<br><br>
 *
 * @author 31528
 * @version 1.0
 */
class PostgreSqlSink(dbSource:String,tableName:String) extends RichSinkFunction[Row]{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var conn:Connection=null
  var ps:PreparedStatement=null
  var  columnMap:util.Map[String,String] =null

  /**
   * Create an connection
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    logger.info(s"=================ClassName:${this.getClass.getName} Beginning Open method:Create an Connection=======================")

    val globalJobParameters: ExecutionConfig.GlobalJobParameters =
      getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val configMap: util.Map[String, String] = globalJobParameters.toMap

    val configPath: String = configMap.get("configPath")
    logger.info(s"=================Load config file:$configPath=================================")
    val pathIn: InputStream = this.getClass.getResourceAsStream(configPath)
    val properties: Properties = new Properties()
    properties.load(pathIn)
    conn=ConnectionBuilder.getConnection(properties,dbSource)
    ps=conn.prepareStatement(s"select * from $tableName where 1=2")
  }

  override def invoke(value: Row, context: SinkFunction.Context): Unit = {
    logger.info(s"==================ClassName:${this.getClass.getName}  Begin invoke: method================================")
    val sql: String = insertTable()
    val insertPS: PreparedStatement = conn.prepareStatement(sql)

    val columnCount: Int = ps.getMetaData.getColumnCount
    logger.info(s"=========invoke method getColumnCount:$columnCount===========")

    //Setting  Values
    for(i <- 0 to columnCount-1){
      //setObject 1-base
      //getField  0-base
      insertPS.setObject(i+1,value.getField(i))
    }
    insertPS.executeUpdate()

  }

  override def close(): Unit = {
    if (conn!=null) conn.close()
    if (ps!=null) ps.close()
  }

  /**
   *  Append insert SQL
   *  eg: INSERT INTO  table (col0,col1,col2..........)
   *                          VALUES(?,?,?,.......)
   * @return
   */
  def insertTable():String={

    logger.info(s"==================ClassName:${this.getClass.getName}  Begin insertTable method================================")
    val insertColumn:StringBuilder=null
    val insertValue:StringBuilder=null
    insertColumn.append(s"INSERT INTO $tableName (")
    insertValue.append("VALUES(")

    val metaData: ResultSetMetaData = ps.getMetaData
    val columnCount: Int = metaData.getColumnCount
    columnMap=new util.LinkedHashMap[String,String]()
    //Getting Column name And Column type From meta data
    for (i <- 0 to columnCount-1){
      // Map(ColumnName,ColumnType))
      columnMap.put(metaData.getColumnName(i),metaData.getColumnTypeName(i))
      insertColumn.append(metaData.getColumnName(i)+",")
      insertValue.append("?,")
    }
    //Delete last ,
    insertColumn.deleteCharAt(insertColumn.lastIndexOf(","))
    insertValue.deleteCharAt(insertValue.lastIndexOf(","))
    //Append )
    insertColumn.append(")")
    insertValue.append(")")
    insertColumn.append(insertColumn)
    val insertSql: String = insertColumn.toString()
    logger.info(s"============Insert PreparedStatement SQL:$insertSql============")
    insertSql
  }

}
