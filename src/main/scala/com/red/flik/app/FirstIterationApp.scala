package com.red.flik.app


import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * <b></b><br>
 *
 * <p>[详细描述]</p>
 *
 * Date: 2022/7/14 10:56<br><br>
 *
 * @author 31528
 * @version 1.0
 */
import org.apache.flink.api.scala._
object FirstIterationApp {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val valueDS: DataStream[Long] = env.generateSequence(0, 10)

    val outDS: DataStream[Long] = valueDS.iterate(
      iteration => {
        val minusOne: DataStream[Long] = iteration.map(v => v - 1)
        val left: DataStream[Long] = minusOne.filter(_%2==1)
        val right: DataStream[Long] = minusOne.filter(_%2==0)
        (left, right)
      }
    )

    outDS.print().setParallelism(1)
    env.execute("Iterate App")
  }
}
