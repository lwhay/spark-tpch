package main.scala.cores

import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 20
  * Wenhai Li <lwh@whu.edu.cn>
  *
  */
class Q20raw4 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => LineitemRaw(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val part = spark.sparkContext.textFile(url + "/part.tbl").map(_.split('|')).map(p => PartRaw(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
    val partsupp = spark.sparkContext.textFile(url + "/partsupp.tbl").map(_.split('|')).map(p => PartsuppRaw(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()

    val forest = udf { (x: String) => x.contains("sandy") }

    val res = part.filter(forest($"p_name"))
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(lineitem, $"ps_suppkey" === lineitem("l_suppkey") && $"ps_partkey" === lineitem("l_partkey"))
      .filter($"l_shipdate" >= "1992-12-05" && $"l_shipdate" < "1992-12-10")
      .select($"ps_suppkey", $"ps_availqty").count()

  }

}
