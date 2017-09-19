package main.scala

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 23
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q23 extends TpchQuery {

  import spark.implicits._

  override def execute(): Unit = {

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.select($"l_orderkey", $"l_linenumber", $"l_suppkey", $"l_partkey", $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax")
      .sort($"l_extendedprice")
      .agg(count($"l_orderkey"))

    outputDF(res)

  }

}
