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
    //  3 创建socket流数据
    val srcData: DataStream[String] = env.socketTextStream("localhost", 7777)
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
        }) //  到此,分配时间戳和水位线的步揍完成
      .keyBy(_._1) //  数据keyBy进行分流
      .timeWindow(Time.seconds(10)) //  开个10s的滚动窗口
      .process(new MyProcess())

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
