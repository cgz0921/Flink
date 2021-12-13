import org.apache.flink.api.common.functions.FilterFunction

object UDF_Demo {
  def main(args: Array[String]): Unit = {
    val ff: UDF_Filter = new UDF_Filter
    println(ff.filter("nihaohelloworld"))
  }

}

class UDF_Filter extends FilterFunction[String] {
  override def filter(t: String): Boolean = {
    t.contains("hello")
  }
}
