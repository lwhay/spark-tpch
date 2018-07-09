package main.scala

import org.apache.spark.sql.functions._

/**
  * Created by michael on 1/3/17.
  * Corresponds to query 28, with the sorting field as l_comment.
  */
class Q32 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val order = spark.sparkContext.textFile(url + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF()

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.select($"l_orderkey", $"l_extendedprice", $"l_linenumber", $"l_comment")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"o_orderdate", $"l_extendedprice", $"l_orderkey", $"l_linenumber", $"l_comment")
      .sort($"l_comment")
      .agg(count($"l_orderkey"))

    outputDF(res)

  }

}
