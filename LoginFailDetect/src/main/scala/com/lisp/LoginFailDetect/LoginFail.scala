package com.lisp.LoginFailDetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFail {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventDS: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430836),
      LoginEvent(1, "192.168.0.2", "fail", 1558430853),
      LoginEvent(1, "192.168.0.3", "fail", 1558430864),
      LoginEvent(2, "192.168.10.10", "fail", 1558430875)
    ))
    loginEventDS.assignAscendingTimestamps(_.eventTime * 1000)
      .filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process( new MatchFunction())
      .print()

    env.execute()


  }
  //自定义KeyedFunction
  class MatchFunction extends KeyedProcessFunction[Long, LoginEvent, String]{
    lazy private val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("loginState", classOf[LoginEvent])
    )
    override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, String]#Context, collector: Collector[String]): Unit = {
      //判断是否在listState中，是比较，不是则加入
      //直接process的话，是会根据key进行聚会添加的，之前是因为有时间窗的进行的聚和
      loginState.add(i)

      // 注册定时器，触发事件设定为2秒后, 实测下来用处不大啊啊啊啊啊啊，相同的key还是直接加入到loginState，然后才执行中断
      //因为所有的数据已经进来了，所以不会触发定时
      context.timerService.registerEventTimeTimer(i.eventTime* 1000 + 2 * 1000)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allLogins: ListBuffer[LoginEvent] = ListBuffer()
      import scala.collection.JavaConversions._
      for (login <- loginState.get) {
        allLogins += login
      }
      loginState.clear()

      if (allLogins.length > 1) {
        out.collect(allLogins.head.toString)

      }
      allLogins.clear()

      Thread.sleep(1000)
    }
  }

}
