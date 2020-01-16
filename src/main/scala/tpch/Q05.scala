package main.scala

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 5
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q05 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val customer = spark.sparkContext.textFile(url + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val nation = spark.sparkContext.textFile(url + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF()
    val region = spark.sparkContext.textFile(url + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toLong, p(1).trim, p(1).trim)).toDF()
    val order = spark.sparkContext.textFile(url + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF()
    val partsupp = spark.sparkContext.textFile(url + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF()
    val supplier = spark.sparkContext.textFile(url + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    val res = region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)

    outputDF(res)

  }

}
