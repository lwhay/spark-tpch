package main.scala

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 13
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q13 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val customer = spark.sparkContext.textFile(url + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
    val order = spark.sparkContext.textFile(url + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF()

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    val res = customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)

    outputDF(res)

  }

}
