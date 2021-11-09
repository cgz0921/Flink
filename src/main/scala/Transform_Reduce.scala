import org.apache.flink.streaming.api.scala._

object Transform_Reduce {
  def main(args: Array[String]): Unit = {
    //  创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //  读取text文件
    val srcData: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt", "UTF-8")

    //  对读取到的源数据进行处理
    val reduceData: DataStream[SensorReading] = srcData
      .map(lineData => {  //  为啥使用map而不使用flatmap那种？因为这一行里面的数据是一个整体,你只能一行一行的进行处理
      val newArrData: Array[String] = lineData.split(",")
      SensorReading(newArrData(0), newArrData(1).toLong, newArrData(2).toDouble)
        //  将每一行的数据封装到SensorReading类中,当然实际生产中我们肯定是要对进来的数据进行判断、过滤的，需要先过滤到脏数据才能进行下面的操作
    }).keyBy("id")  //  根据类中的某个字段进行分组
      .reduce((x, y) => {     //  对分组后的数据进行reduce操作
        SensorReading(x.id, x.timestamp + 1, y.temperature) //  id肯定都是相同的
      })


    //  数据输出进行查看,这里有一个问题就是：即便并行度设置为1，文件中的数据顺序和输出的数据也是有区别(混乱)的，后面我们会重点学习如何解决这个问题
    reduceData.print().setParallelism(1)

    //  运行程序并给它一个任务名称
    env.execute("reduce_job")
  }
}
/*
reduce函数---进去2个同类型的数据,出来一个同类型的数据
 */