package com.lisp.LoginFailDetect

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventDS: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430862),
      LoginEvent(1, "192.168.0.2", "fail", 1558430863),
      LoginEvent(1, "192.168.0.3", "fail", 1558430864),
      LoginEvent(2, "192.168.0.3", "fail", 1558430874),
      LoginEvent(2, "192.168.10.10", "fail", 1558430875)
    ))
    val logEventByKey: KeyedStream[LoginEvent, Long] = loginEventDS.assignAscendingTimestamps(_.eventTime * 1000).keyBy(_.userId)

    //定义一个匹配模式，next紧邻事件。还有哟个followby // 定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(logEventByKey, loginFailPattern)
    // .select方法传入一个 pattern select function，当检测到定义好的模式序列时就会调用
    import scala.collection.Map
    patternStream.select(
      (pattern: Map[String, Iterable[LoginEvent]]) => {
        //根据Iterable内的数量来确定执行次数，也就是匹配成功的次数
        val next = pattern.getOrElse("next", null).iterator.next()
//        println("***" + pattern.size)
        (next.userId, next.ip, next.eventTime)
      }
    ).print()

    env.execute("Detect")
  }
}
