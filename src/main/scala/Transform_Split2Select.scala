import org.apache.flink.streaming.api.scala._

object Transform_Split2Select {
  def main(args: Array[String]): Unit = {
    //  创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //  读取text文件
    val srcData: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt", "UTF-8")

    //  数据处理
    val splitData: SplitStream[SensorReading] = srcData.map(data => {
      val arrData: Array[String] = data.split(",")
      SensorReading(arrData(0), arrData(1).toLong, arrData(2).toDouble)
    }).split(sensorData => {
      if (sensorData.temperature > 30) {
        Seq("高温")
      } else {
        Seq("低温")
      }
    })

    //  使用select进行数据输出
    val high: DataStream[SensorReading] = splitData.select("高温")
    val low: DataStream[SensorReading] = splitData.select("低温")
    val all: DataStream[SensorReading] = splitData.select("高温", "低温")

//    high.print()
//    low.print()
//    all.print()

    val warning: DataStream[(String, Double)] = high.map(data => {
      (data.id, data.temperature)
    })  //  将SensorReading数据转变为一个元祖

    //  使用connect将SensorReading、元祖合并
    val connectData: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)

    //  使用map/flatMap对connect之后的数据进行分别处理
    val connResult: DataStream[Product] = connectData.map(
      (warningData: (String, Double)) => (warningData._1, warningData._2, "warning"),
      (lowData: SensorReading) => (lowData.id, "health")
    )

    //  打印一下看看
//    connResult.print("connect-")

    //  使用union对两个以上的数据对象进行处理(数据类型必须一致)
    val unionData: DataStream[SensorReading] = high.union(low, all)

    //  打印一下看看
    unionData.print("union-")

    //  执行程序
    env.execute()
  }
}
/*
connect的特点:
  1.只能连接两个对象、且两个对象的类型可以不同、且都保持自身数据的独立性
  2.后续使用map/flatMap对合并后的数据进行独立处理

union的特点:
  1.可以连接多个对象、且多个对象的类型必须相同、union之后数据混合在一起成为辛的数据对象、但数据类型还是一样(因为本身的数据类型就是一样的)

 */