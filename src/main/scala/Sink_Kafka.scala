import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Sink_Kafka {
  def main(args: Array[String]): Unit = {
    // 1 创建流的执行环境--注意导包要正确
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 读取文本文件
    val srcData: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt", "UTF-8")
    // 3 处理读取到数据
    val midData: DataStream[String] = srcData.map(lineData => {
      val arrData: Array[String] = lineData.split(",")
      CaseClass(arrData(0), arrData(1).toLong, arrData(2).toDouble).toString  // 这里将样例类包裹的数据转换成String类型，是为了写入Kafka，因为Kafka的数据一般存储的就是String类型
    })
    // 4 将处理后的数据存储到Kafka中
    midData.addSink(
      new FlinkKafkaProducer011[String](
        "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092",
        "cgz_sinkFlinkKafka",
        new SimpleStringSchema()) //  最后一个参数就是指明数据是以String类型存储
    )

    // 5 执行flink程序
    env.execute("flink2kafka")
  }
}
case class CaseClass(id:String,ts:Long,temp:Double)


/*
针对Kafka数据的操作：
如果需要从Kafka消费数据，需要使用Kafka的consumer
如果需要写入数据到Kafka，需要使用Kafka的producer（正如我们此程序中所做的那样）

在两个窗口页面分别开启生产者、消费者
开启生产者：
/usr/local/kafka/bin/kafka-console-producer.sh --broker-list nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092 --topic cgz_sinkFlinkKafka
开启消费者：
/usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181 --topic cgz_sinkFlinkKafka
然后执行该程序，即可看到在consumer界面出现如下内容：
CaseClass(sensor_7,1547718202,26.7)
CaseClass(sensor_6,1547718201,15.4)
CaseClass(sensor_1,1547718888,15.8)
CaseClass(sensor_8,1547718805,28.1)
CaseClass(sensor_8,1547718505,18.1)
CaseClass(sensor_1,1547718258,25.8)
 */