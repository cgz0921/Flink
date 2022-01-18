import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object Watermark_Demo1 {
  def main(args: Array[String]): Unit = {
    //  1 创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  2 设置时间特性为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  还可以设置watermark的生成周期(每隔5s生成一个watermark)
    env.getConfig.setAutoWatermarkInterval(5000)
    //  3 创建socket流数据
    val srcData: DataStream[String] = env.socketTextStream("localhost", 9999)
    //  4 对原始刘数据进行处理
    val resData: DataStream[String] = srcData
      .map(lineData => {
        val arrData: Array[String] = lineData.split(",")
        //  返回一个元祖类型的数据，因为事件时间的单位必须是毫秒，所以我们输入数据的第二个元素需要进行转化成毫秒
        (arrData(0), arrData(1).toLong * 1000L)

      })
      //  分配时间戳和水位线一定要在keyBy之前，因为你要在keyBy之后再分配，可能不同的数据就跑到不同的slot里面去了
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)]
        (Time.seconds(5)) //  设置最大延迟时间为5s
        { //  告诉系统，你需要的时间戳EventTime是元祖的第二个元素
          override def extractTimestamp(arr: (String, Long)): Long = arr._2
        }) //  到此,分配时间戳和水位线的步骤完成
      .keyBy(_._1) //  数据keyBy进行分流
      .timeWindow(Time.seconds(10)) //  开个10s的滚动窗口
      .process(new MyProcess())

      srcData.print()
      resData.print()
    //  执行流程序
    env.execute("第一个Flink-Watermark程序")
  }

}
class MyProcess extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
    out.collect(new Timestamp(context.window.getStart)+"~"+new Timestamp(context.window.getEnd)+" 这段窗口内共有  "+elements.size+" 个元素！")
  }
}

/*
设置了watermark的程序什么时候触发计算呢？首先需要了解一下几个知识点：
  1.flink程序默认200ms生成一个水位线
  2.触发计算的水位线 = 系统读取到的最大EventTime - 设置的最大延迟时间
  3.flink的时间窗口内包含的数据是左闭右开、即[0,10)、[10,20)、[20,30)、[30,40)......

综上所述，设置了watermark的程序触发计算的条件是：
1-系统读取到的最大EventTime - 设置的最大延迟时间 >= 水位线
2-水位线已经生成(默认200ms生成一次)
 */
