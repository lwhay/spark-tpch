package main.scala.tpch

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 15
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q15 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val supplier = spark.sparkContext.textFile(url + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val revenue = lineitem.filter($"l_shipdate" >= "1996-01-01" &&
      $"l_shipdate" < "1996-04-01")
      .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"l_suppkey")
      .agg(sum($"value").as("total"))
    // .cache

    val res = revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")

    outputDF(res)

  }

}
