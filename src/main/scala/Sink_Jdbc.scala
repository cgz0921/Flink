import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object Sink_Jdbc {
  def main(args: Array[String]): Unit = {
    // 1  创建Flink执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 为了保证更新语句生效，必须将并行度设置为1，否者会出现相同ID存在多条记录的情况
    // 2  source+transform  读取文件并取我们需要的字段数据封装到样例类中
    val srcData: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt", "UTF-8")

    val midData: DataStream[SensorScheme] = srcData.map((lineData: String) => {
      val arrData: Array[String] = lineData.split(",")
      SensorScheme(arrData(0), arrData(2).toDouble)
    })
    // 3  sink  将数据写入到mysql数据库
    midData.addSink(new MysqlSink())

    // 4  执行flink程序
    env.execute("JDBC")
  }
}

//  需要用到的样例类
case class SensorScheme(id:String,temperature:Double)

//  自定义jdbc的sink类
class MysqlSink() extends RichSinkFunction[SensorScheme]{
  //  以下三个变量的类型要手动指定,不然不会自动填充的,如果没有指定类型会报错
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _

  //  重写open方法：目的是创建mysql连接、定义sql语句
  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
    conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/cgz?useSSL=false","root","1989cgz0921")
    insertStmt=conn.prepareStatement("insert into flink_sink(id,temperature) values (?,?)")
    updateStmt=conn.prepareStatement("update flink_sink set temperature=? where id=?")
  }

  //  重写invoke方法：真正去执行上面open()方法定义的sql语句
  override def invoke(value: SensorScheme, context: SinkFunction.Context[_]): Unit = {
    //  先执行update语句
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    //  如果表中没有相同id的数据可以去更新,那么就执行insert语句
    if (updateStmt.getUpdateCount==0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  //  重写close方法：依次关闭sql语句、mysql连接
  override def close(): Unit = {
//    super.close()
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}

/*
create table flink_sink(
  id varchar(20),
  temperature double);
 */

