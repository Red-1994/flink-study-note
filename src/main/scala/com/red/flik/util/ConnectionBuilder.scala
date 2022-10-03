package com.red.flik.util

import java.sql.{Connection, DriverManager}
import java.util
import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}
/**
 * <b>Create an connection from JDBC</b><br>
 *
 * <p></p>
 *
 * Date: 2022/7/14 16:35<br><br>
 *
 * @author 31528
 * @version 1.0
 */
object ConnectionBuilder {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getConnection(configMap: util.Map[String, String],dbSource:String):Connection={
     val driver:String= configMap.get(s"jdbc.$dbSource.driver")
     val url:String= configMap.get(s"jdbc.$dbSource.url")
     val user:String= configMap.get(s"jdbc.$dbSource.user")
     val password:String= configMap.get(s"jdbc.$dbSource.password")
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, user, password)
    logger.info(s"Loading driver  create an Connection=$connection")
    connection
  }

  def getConnection(configProp:Properties, dbSource:String):Connection={
    val driver:String= configProp.getProperty(s"jdbc.$dbSource.driver")
    val url:String= configProp.getProperty(s"jdbc.$dbSource.url")
    val user:String= configProp.getProperty(s"jdbc.$dbSource.user")
    val password:String= configProp.getProperty(s"jdbc.$dbSource.password")
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, user, password)
    logger.info(s"Loading driver  create an Connection=$connection")
    connection
  }
}
