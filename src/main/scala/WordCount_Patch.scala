import org.apache.flink.api.scala._

object WordCount_Patch {
  def main(args: Array[String]): Unit = {
    //  创建Flink执行环境(批处理环境使用ExecutionEnvironment)
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //  读取离线文件
    val lines: DataSet[String] = env.readTextFile("src/main/resources/word.txt")
    //  处理读到的文件(flink中没有reduceByKey,批处理中使用groupBy进行分组,流处理中使用keyBy分组)
    val result: AggregateDataSet[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    //  打印结果
    result.print()
  }

}
