package main.scala

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 8
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q08 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val customer = spark.sparkContext.textFile(url + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val nation = spark.sparkContext.textFile(url + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF()
    val region = spark.sparkContext.textFile(url + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toLong, p(1).trim, p(1).trim)).toDF()
    val order = spark.sparkContext.textFile(url + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF()
    val part = spark.sparkContext.textFile(url + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
    val supplier = spark.sparkContext.textFile(url + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }

    val fregion = region.filter($"r_name" === "AMERICA")
    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume")).
      join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_suppkey" === nat("s_suppkey"))

    val res = nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(getYear($"o_orderdate").as("o_year"), $"volume",
        isBrazil($"n_name", $"volume").as("case_volume"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"))
      .sort($"o_year")

    outputDF(res)

  }

}
