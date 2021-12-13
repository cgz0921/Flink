import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream._

object DataType_Demo {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 2.测试数据类型
    val data1: DataSet[Int] = env.fromElements(1, 2, 3)
    val data2: DataSet[Long] = env.fromElements(1L, 2L, 3L)
    val data3: DataSet[Double] = env.fromElements(1.1, 2.2, 3.3)
    val data4: DataSet[(String, Int)] = env.fromElements(("cgz", 25), ("lxl", 30))
    val data5: DataSet[Stu] = env.fromElements(Stu("cgz", 25), Stu("lxl", 30))
    // 3.打印数据
    println(data1)
    println(data2)
    println(data3)
    println(data4)
    println(data5)
  }

}
case class Stu(name:String,age:Int)
