package com.lisp.NetWorkTraffic

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入的数据格式
case class ApacheLogEvent(ip:String, userId:String, eventTime:Long, method:String, url:String )
//输出的数据格式
case class urlViewCount(url:String, windowEnd: Long, count: Long)

object Network {
  def main(args: Array[String]): Unit = {
    //env 创建一个 StreamExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义Time 类型  设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //Source
    val steam: DataStream[ApacheLogEvent] =
      env.readTextFile("E:\\IDEAworkplace\\UserBehaviorAnalysis\\NetWorkTraffic\\src\\main\\resources\\apache.log")
        .map(line => {
          val lineArray: Array[String] = line.split(" ")
          ApacheLogEvent(lineArray(0), lineArray(1), new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(lineArray(3)).getTime, lineArray(5), lineArray(6))
        })
    //指定时间戳
    val DStimeStamps: DataStream[ApacheLogEvent] = steam.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = {
        t.eventTime
      }
    })

    val keyDS = DStimeStamps.filter(_.method == "GET").keyBy(_.url)
    //设置滑动窗口，聚合了窗口内的数据，但把最终所以窗口数据还是以流的形式放回去，所以你要加入窗口标签。
    val winResult2ItemViewCountDS = keyDS.timeWindow(Time.seconds(60), Time.seconds(5))
      .aggregate(new CountAgg, new windowResultFunction())
      .keyBy(_.windowEnd)
      .process( new TopNHotURL(5))
        .print()

    env.execute("job")
  }

  //自定义聚合函数
  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  //自定义window Function
  class windowResultFunction extends WindowFunction[Long, urlViewCount, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[urlViewCount]): Unit = {
      val url = key
      val count: Long = input.iterator.next()
      out.collect(urlViewCount(url, window.getEnd, count))
    }
  }


  //前面keyby，那么直接操作啊，.process干啥。
  class TopNHotURL(topSize: Int) extends KeyedProcessFunction[Long, urlViewCount, String] {
    //这个ListState是哪里来的啊，从上面的KeyStream中获得的
    lazy private val ulrState: ListState[urlViewCount] =
      getRuntimeContext.getListState(new ListStateDescriptor[urlViewCount]("urlState", classOf[urlViewCount]))


    override def processElement(i: urlViewCount, context: KeyedProcessFunction[Long, urlViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      ulrState.add(i)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      //注册定时器，当窗口收集完成是，windowEnd + 1   如果是乱序的话就会包含其他的比他小的窗口值。
      context.timerService().registerEventTimeTimer(i.windowEnd + 1000)
    }

    //定时器触发操作，取TopNo
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, urlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //      super.onTimer(timestamp, ctx, out)
      //怎么从State中取数据呢
      val allURLs: ListBuffer[urlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._

      for (item <- ulrState.get()) {
        allURLs += item
      }
      //清除状态中的数据，释放空间
      //滑动窗口数据是会有重叠的的啊，这样清除不影响吗。每个窗口都自己维护一份？ 以windowEnd为标志，窗口不一。
      ulrState.clear()
      // 按照点击量从大到小排序
      val sortedURLs = allURLs.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1000)).append("\n")

      for (i <- sortedURLs.indices) {
        val current: urlViewCount = sortedURLs(i)
        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i + 1).append(":")
          .append("  访问的=").append(current.url)
          .append("  浏览量=").append(current.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }
}
