package FlinkTable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{table2RowDataSet, table2RowDataStream, _}

object Table_Demo1 {
  def main(args: Array[String]): Unit = {
    //  1.创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  2.创建Table即FlinkSQL执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val input: DataStream[String] = env
      .readTextFile("src/main/resources/sensor.txt")
    //  3.创建表
    //  3.1 根据文件创建一张表(貌似不可以)
//    val textTable: Table = tableEnv.from("sensor_1,1547718199,35.8")
    //  3.2 根据DataStream创建一张表
    val dsData: DataStream[FlinkTableDS] = input
      .map(lineData => {
        val str: Array[String] = lineData.split(",")
        FlinkTableDS(str(0), str(1).toLong, str(2).toDouble)
      })

    val dsTable: Table = tableEnv.fromDataStream(dsData)

    //  4.操作表
    //  4.1 打印表结构
    dsTable.printSchema()
    //  4.2 查询表数据-TableAPI方式
    val res_table1: Table = dsTable.select("id,temperature").filter("id == 'sensor_1'")
    //  4.2 查询表数据-SQL语句方式
    val res_table2: Table = tableEnv.sqlQuery("select id,temperature from " + dsTable + " where id = 'sensor_1'")
    //  4.3 查询结果打印（只能先把Table类型的结果转换成DataStream类型，然后打印出DataStream结果，因为Table类型的只能打印出表结构）
    //  Table类型转换成DataStream类型--需要导入隐式转换  import org.apache.flink.table.api.scala._
    val res_table1_ds: DataStream[(String, Double)] = res_table1.toAppendStream[(String, Double)]
    val res_table2_ds: DataStream[(String, Double)] = res_table2.toAppendStream[(String, Double)]

    //  4.4 打印转换后的DS结果
    res_table1_ds.print()
    res_table2_ds.print()


    //  5.执行流式程序
    env.execute("FlinkTable入门流程")
  }

}

case class FlinkTableDS(
                        id:String,
                        ts:Long,
                        temperature:Double
                      )
