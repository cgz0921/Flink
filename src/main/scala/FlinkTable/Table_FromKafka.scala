package FlinkTable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object Table_FromKafka {
  def main(args: Array[String]): Unit = {
    //  1 创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  2 创建TableAPI执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //  3 连接外部系统并读取数据--从Kafka读取数据
    tableEnv
      .connect(new Kafka()
        .version("0.11")  //  指定Kafka的版本
        .topic("flink_read_kafka")  //  指定要读取Kafka的topic
        .property("bootstrap.servers","nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092") //  Kafka地址
        .property("zookeeper.connect","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181") //  zookeeper地址
      )
      .withFormat(new Csv())  // 指定读取文件的格式，因为我们使用逗号,进行分割，所以可以将它定义为csv文件
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("ts",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")
    //  4 读取表数据
    val res_table: Table = tableEnv.from("kafkaInputTable")
    //  5 将读取到的 Table 类型结果转换成 DataStream 数据
    val res_ds: DataStream[(String, Long, Double)] = res_table.toAppendStream[(String, Long, Double)]
    //  6 将DataStream类型的结果打印输出
    res_ds.print()
    //  7 执行流式程序
    env.execute()
  }

}
