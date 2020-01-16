package main.scala

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 12
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q12 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val order = spark.sparkContext.textFile(url + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF()

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    val res = lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")), sum(lowPriority($"o_orderpriority")))
      .sort($"l_shipmode")

    outputDF(res)

  }

}
