import org.apache.flink.streaming.api.scala._

object WordCount_Stream {
  def main(args: Array[String]): Unit = {
    //  创建Flink执行环境(流式环境使用StreamExecutionEnvironment)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8) //  当然了、我们还可以自己定义并行度
    //  从socket中读取流数据
    val srcData: DataStream[String] = env.socketTextStream("localhost", 9999)
    //  对流数据进行处理(flink中没有reduceByKey,批处理中使用groupBy进行分组,流处理中使用keyBy分组)
    val result: DataStream[(String, Int)] = srcData.flatMap(_.split("\\W+")).map((_, 1)).keyBy(0).sum(1)
    //  结果输出(输出时再次指定并行度)
    result.print().setParallelism(1)
    //  因为是流式数据、所以需要开启执行、程序才能运行
    env.execute()
  }

}
