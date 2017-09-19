package main.scala

import org.apache.spark.sql.functions._

/**
  * Created by michael on 12/15/16.
  */
class Q26 extends TpchQuery {

  import spark.implicits._

  override def execute(): Unit = {

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.select($"l_orderkey", $"l_linenumber", $"l_suppkey", $"l_partkey", $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax", $"l_returnflag", $"l_linestatus", $"l_shipdate", $"l_commitdate", $"l_receiptdate", $"l_shipinstruct", $"l_shipmode", $"l_comment")
      .sort($"l_extendedprice")
      .agg(count($"l_orderkey"))

    outputDF(res)

  }
}