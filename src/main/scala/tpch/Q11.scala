package main.scala.tpch

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 11
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q11 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val nation = spark.sparkContext.textFile(url + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF()
    val partsupp = spark.sparkContext.textFile(url + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF()
    val supplier = spark.sparkContext.textFile(url + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    val res = tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)

    outputDF(res)

  }

}
