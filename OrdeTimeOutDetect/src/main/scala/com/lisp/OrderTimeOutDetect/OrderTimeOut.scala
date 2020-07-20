package com.lisp.OrderTimeOutDetect

import org.apache.flink.cep.PatternTimeoutFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
//      OrderEvent(1, "pay", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000).keyBy(_.orderId)

    //定义pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //定义一个输出标签，用于标明侧输出流
    val orderTimeOutput: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")

    //从keyby 之后的每条流中匹配定义好的模式。
    val pattStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    //从pattern stream中获得输出流
    //PatternTimeoutFunction是不匹配的流，patternSelectFunction是匹配的流。我们要的是匹配了begin的但没有follow
    val completedResultDataStream: DataStream[OrderResult] = pattStream.select(orderTimeOutput)(
      //对于已超时的部分模式匹配的事件序列，会调用这个函数
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val orderEvent = pattern.getOrElse("begin", null).iterator.next()
        OrderResult(orderEvent.orderId, "timeOut")
      }
    )(
      // 检测到定义好的模式序列时，就会调用这个函数
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val orderEvent = pattern.getOrElse("begin", null).iterator.next()
        OrderResult(orderEvent.orderId, "success")
      }
    )
    //打印出来的是匹配的事件序列
    completedResultDataStream.print()

    //这个是timeOut结果
//    completedResultDataStream.getSideOutput(orderTimeOutput).print()

    env.execute("Order Timeout Detect")
  }
}
