import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Window_Demo {
  def main(args: Array[String]): Unit = {
    // 1 创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 2 创建socket流进行模拟数据
    val socketData: DataStream[String] = env.socketTextStream("localhost", 7777)
    // 3 对抓取到的socket数据进行处理--并使用窗口操作
    val midData: DataStream[FlinkWindow] = socketData
      .map(lineData => {
        val arrData: Array[String] = lineData.split(",")
        FlinkWindow(arrData(0), arrData(1).toLong, arrData(2).toDouble)
      })

    val windowData: DataStream[FlinkWindow] = midData
      .keyBy("id")
//      .timeWindow(Time.seconds(15)) //  滚动窗口--每一个窗口时间内的数据都是独立的，不会重合计算或者打印
//      .timeWindow(Time.seconds(15),Time.seconds(5)) //  滑动窗口--有两个参数，参数1表示窗口大小，参数2表示滑动步长，如果参数1大于参数2，则窗口间的数据会重复计算和打印
//      .window(EventTimeSessionWindows.withGap(Time.minutes(1L)))  //  会话窗口--这个的用法和滚动、滑动窗口有所不同，需要记住其写法，里面的时间代表的是这个session关闭超过1分钟就开始计算
//      .countWindow(5) //  滚动统计窗口，当key的累计值达到定义的数值就触发计算
      .countWindow(9,3) //  滑动统计窗口，不知道理解的对不对(如果key的累计值没有达到指定的计算数值，那么就看窗口是不是滑动了，如果滑动了也触发计算)
      .reduce((r1, r2) => {
        FlinkWindow(r1.id, r1.ts, r1.temperature.min(r2.temperature)) //  取两个对象中温度值最小的值
      })
    // 4 打印出来数据看看效果
    midData.print("data")
    windowData.print("result")
    // 5 执行flink程序
    env.execute("flink程序的window操作")
  }
}

case class FlinkWindow(
                      id:String,
                      ts:Long,
                      temperature:Double
                      )
