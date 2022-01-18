package FlinkTable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object Table_FromFile {
  val schema: Schema = new Schema

  def main(args: Array[String]): Unit = {
    //  1 创建流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  2 创建TableAPI的执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //  3 连接外部存储系统并读取数据-读取text文件(csv)
    tableEnv
      .connect(new FileSystem().path("src/main/resources/sensor.txt"))  //  设置要读取文件的地址
      .withFormat(new Csv())                                                  //   设置要读取文件的格式--要使用Csv类，需要在pom文件中添加flink-csv依赖
      .withSchema(
        schema
          .field("id",DataTypes.STRING())
          .field("ts",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      )                                                                      //     设置根据读取到的文件创建的表的字段及类型
      .createTemporaryTable("inputTable")                             //     创建表

    //  4 查询表中数据
    //  4-1 使用from方式
    val res_table1: Table = tableEnv.from("inputTable")
    //  4-2 使用sql查询的方式
    val res_table2: Table = tableEnv.sqlQuery("select * from inputTable")
    //  5 将查询的Table类型的结果转换成DataStream类型
    val res_ds1: DataStream[(String, Long, Double)] = res_table1.toAppendStream[(String, Long, Double)]
    val res_ds2: DataStream[(String, Long, Double)] = res_table2.toAppendStream[(String, Long, Double)]
    //  6 DS类型的结果进行输出
    res_ds1.print()
    res_ds2.print()
    //  7 执行流式程序
    env.execute()
  }
}
