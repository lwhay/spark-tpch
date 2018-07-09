package main.scala.cores

import org.apache.spark.sql.functions.sum

/**
  * TPC-H Query 6
  * Wenhai Li <lwh@whu.edu.cn>
  *
  */
class Q06raw2 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(INPUT_DIR + "/lineitem.tbl").map(_.split('|')).map(p => LineitemRaw(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()

    val res = lineitem.filter($"l_shipdate" >= "1993-01-01" && $"l_shipdate" < "1994-01-01" && $"l_discount" >= 0.02 && $"l_discount" <= 0.04 && $"l_quantity" < 13)
      .agg(sum($"l_extendedprice" * $"l_discount"))

    outputDF(res)

  }

}
