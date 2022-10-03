package com.red.flik.app

import com.red.flik.sink.PostgreSqlSink
import com.red.flik.source.PostgreSqlSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}
/**
 * <b>一句话简述此类的用途</b><br> 
 *
 * <p>[详细描述]</p>
 *
 * Date: 2022/7/15 14:49<br><br>
 *
 * @author 31528
 * @version 1.0
 */
object PostgreSqlEtlTestApp {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    logger.info(s"==========ClassName:${getClass.getName} Beginning Main=================")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //Disable Kro serializers
//    env.getConfig.disableGenericTypes
//    env.getConfig.enableForceAvro
//      env.registerType(new Row())
    val toolArgs: ParameterTool = ParameterTool.fromArgs(args)
    val configPath: String = toolArgs.get("configPath", "/config.properties")
    val conf = new Configuration()
    conf.setString("configPath",configPath)
    logger.info(s"Setting GlobalJobParameters configPath:$configPath")
    env.getConfig.setGlobalJobParameters(conf)
    val sourceTable:String="dwd.dwd_quality_sqam_claim_form_header"
    val targetTable:String="dwd.dwd_quality_sqam_claim_form_header_test"
    val inputDS: DataStream[Row] = env.addSource(new PostgreSqlSource("dws", sourceTable))


    val outPutDS: DataStreamSink[Row] = inputDS.addSink(new PostgreSqlSink("dws", targetTable))

    env.execute("PostgreSqlEtlTestApp")

  }
}
