package FlinkTable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object Table_Select {
  def main(args: Array[String]): Unit = {
    //  1 创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  2 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //  3 根据text文件创建一张表
    tableEnv
      .connect(new FileSystem().path("src/main/resources/sensor.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("ts",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("tableOfSelect")
    //  4 读取表
    //  4.1 使用TableAPI读取
    val midTable: Table = tableEnv.from("tableOfSelect")
    val res_table01: Table = midTable
      .select('id, 'temperature)  //  这里使用一个单引号-1：可以查询多个字段  2：导入隐式转换 import org.apache.flink.table.api.scala._
      .filter('id === "sensor_1")  //  这里使用一个单引号-进行过滤的时候使用三等号===表示等于

    //  4.1-1 使用TableAPI聚合查询
    val res_table_agg01: Table = midTable
      .groupBy('id)
      .select('id, 'id.count as 'cn)

    //  4.2 使用SQL方式读取
    //  4.2.1 双引号方式SQL--适合简单、短SQL语句查询
    val res_table02: Table = tableEnv.sqlQuery("select * from tableOfSelect where id='sensor_1'")
    //  4.2.1-1 使用SQL聚合查询
    val res_table_agg02: Table = tableEnv.sqlQuery("select id,count(*) as cn from tableOfSelect group by id")
    //  4.2.2 三引号方式SQL--适合复杂、长SQL语句查询
    val res_table03: Table = tableEnv.sqlQuery(
      """
        |select
        |id,ts,temperature
        |from
        |tableOfSelect
        |where
        |id='sensor_1'
        |""".stripMargin)

    //  5 读取表结果转换为DataStream
    val res_ds01: DataStream[(String, Double)] = res_table01.toAppendStream[(String, Double)]
    val res_ds02: DataStream[(String, Long, Double)] = res_table02.toAppendStream[(String, Long, Double)]
    val res_ds03: DataStream[(String, Long, Double)] = res_table02.toAppendStream[(String, Long, Double)]
    //  下面需要注意：
    //  因为咱们执行的是流计算，所以进行聚合操作的时候，相同key的聚合结果是在不停的发生变化的，
    //  所以此时我们需要使用toRetractStream方法，如果继续使用toAppendStream方法是会报错的
    //  toRetractStream方法返回的类型的第一个类型是Boolean、第二个就是我们熟悉的结果类型（元祖）
    //  我们只需要看Boolean=true的结果即可，true代表后面的结果已经更新了
    val res_ds_agg01: DataStream[(Boolean, (String, Long))] = res_table_agg02.toRetractStream[(String, Long)]
    val res_ds_agg02: DataStream[(Boolean, (String, Long))] = res_table_agg02.toRetractStream[(String, Long)]


    //  6 DataStream类型结果打印输出
    res_ds01.print()
    res_ds02.print()
    res_ds03.print()
    res_ds_agg01.print()
    res_ds_agg02.print()
    //  7 流程序执行
    env.execute()
  }

}
