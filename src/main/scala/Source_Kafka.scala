import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

object Source_Kafka {
  def main(args: Array[String]): Unit = {
    //  首先创建Flink的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //  设置Kafka的连接信息
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers","nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")             //  Kafka集群信息
    props.setProperty("group.id","cgz_Flink")                                                           //  定义消费者组、避免生产事故
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")    //  key的序列化类型
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")  //  value的序列化类型
    props.setProperty("auto.offset.reset","earliest") //  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费

    //  连接Kafka数据
    val kfkData: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("cgz_Topic_Flink", new SimpleStringSchema(), props))

    //  直接输出
    kfkData.print()

    //  开始运行程序
    env.execute()
  }

}
