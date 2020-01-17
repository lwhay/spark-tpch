package main.scala.tpch

import org.apache.spark.sql.functions.min

/**
  * TPC-H Query 2
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q02 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val nation = spark.sparkContext.textFile(url + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF()
    val region = spark.sparkContext.textFile(url + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toLong, p(1).trim, p(1).trim)).toDF()
    val part = spark.sparkContext.textFile(url + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
    val partsupp = spark.sparkContext.textFile(url + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF()
    val supplier = spark.sparkContext.textFile(url + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    val res = brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)

    outputDF(res)

  }

}
