package main.scala.cores

import org.apache.spark.sql.functions.{sum, udf}

/**
  * TPC-H Query 14
  * Wenhai Li <lwh@whu.edu.cn>
  *
  */
class Q14raw4 extends TpchQueryRaw {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => LineitemRaw(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val part = spark.sparkContext.textFile(url + "/part.tbl").map(_.split('|')).map(p => PartRaw(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    val res = part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1991-11-25" && $"l_shipdate" < "1994-01-09")
      .select($"p_type", $"l_extendedprice", $"l_discount").count()

  }

}
