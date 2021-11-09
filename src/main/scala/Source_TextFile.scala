import org.apache.flink.streaming.api.scala._

object Source_TextFile {
  def main(args: Array[String]): Unit = {
    //  创建执行环境(流)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //  从文件中读取数据
    val textData: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt", "UTF-8")

    //  对数据进行逻辑处理
//    textData.map(data=>{
//      val dataArray: Array[String] = data.split(",")
//      if (dataArray(2).toDouble > 30){
//        println("high temperature:高温")
//      }else println("low temperature:低温")
//    })


    val value: DataStream[SensorReading] = textData
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }).keyBy("id")
      .filter(_.temperature > 30)

    value.print()

    //  运行程序
    env.execute()
  }
}
// 这里可以不用再重新定义样例类SensorReading，因为这个程序和Source_Collection程序在同一个包下,所以可以访问别的程序下面的类和属性
//case class SensorReading(
//                          id: String,
//                          timestamp: Long,
//                          temperature: Double
//                        )
