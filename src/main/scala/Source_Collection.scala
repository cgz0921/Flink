import org.apache.flink.streaming.api.scala._

object Source_Collection {
  def main(args: Array[String]): Unit = {
    //  创建执行环境(流)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //  从集合中读取数据
    val streamData: DataStream[SensorReading] = env
      .fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 26.7),
      SensorReading("sensor_8", 1547718205, 38.1)
    ))

    //  打印
    streamData.print("streamData")

    //  打印到一个task中(前缀名称相同)
    streamData.print("streamData_Only").setParallelism(1)

    //  启动程序
    env.execute()

  }

}

case class SensorReading(
                          id: String,
                          timestamp: Long,
                          temperature: Double
                        )
