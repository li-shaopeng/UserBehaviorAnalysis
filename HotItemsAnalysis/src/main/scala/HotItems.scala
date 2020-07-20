
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 输出数据样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )

object HotItems {
  def main(args: Array[String]): Unit = {
    //env 创建一个 StreamExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //定义Time 类型  设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //Source
    val steam: DataStream[UserBehavior] =
      env.readTextFile("E:\\IDEAworkplace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val lineArray: Array[String] = line.split(",")
        UserBehavior(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3), lineArray(4).toLong)
      })
    //指定时间戳
    val DStimeStamps: DataStream[UserBehavior] = steam.assignAscendingTimestamps(_.timestamp * 1000)

    val keyDS: KeyedStream[UserBehavior, Tuple] = DStimeStamps.filter(_.behavior == "pv")
      .keyBy("itemId")
    //设置滑动窗口，聚合了窗口内的数据，但把最终所以窗口数据还是以流的形式放回去，所以你要加入窗口标签。
    val winResult2ItemViewCountDS: DataStream[ItemViewCount] = keyDS.timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new windowResultFunction())

    //这边获得的DataStream[ItemViewCount]中可能包含了每个窗口的结果，都在同一个DS里面
    //以至于你都不知道怎么单独对一个窗口进行操作，故采用process。
    //Flink根据事件驱动的，Spark那边是时间，故它的一个DS就是当前窗口的数据（一分钟刷新一次） 直接存入数据库。
    winResult2ItemViewCountDS.keyBy("windowEnd").process(new TopNHotItems(3)).print()

    env.execute("Job")


  }

  //自定义聚合函数
  class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }
  //自定义window Function
  class windowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count: Long = input.iterator.next()
      out.collect(ItemViewCount(itemId,window.getEnd,count))
    }
  }
  //自定义实现process function; 这个功能是什么，有必要这么麻烦吗，去写更底层的函数
  //前面keyby，那么直接操作啊，.process干啥。
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
    //设立一个状态保存list
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val itemStateDesc: ListStateDescriptor[ItemViewCount] = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
      //还得通过上下文来获取状态list？ 该list是程序后台维护的，所以需要被动获取
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      itemState.add(i)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      //注册定时器，当窗口收集完成是，windowEnd + 1   如果是乱序的话就会包含其他的比他小的窗口值。
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }

    //定时器触发操作，取TopNo
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
//      super.onTimer(timestamp, ctx, out)
      //怎么从State中取数据呢
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._

      for(item <- itemState.get()){
        allItems += item
      }
      //清除状态中的数据，释放空间
      //滑动窗口数据是会有重叠的的啊，这样清除不影响吗。每个窗口都自己维护一份？ 以windowEnd为标志，窗口不一。
      itemState.clear()
      // 按照点击量从大到小排序
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for(i <- sortedItems.indices){
        val currentItem: ItemViewCount = sortedItems(i)
        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }

}
