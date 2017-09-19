package main.scala

import org.apache.spark.sql.functions._

/**
  * Created by michael on 12/15/16.
  * Corresponds to query 25 with the sorting field as l_comment.
  */
class Q31 extends TpchQuery {

  import spark.implicits._

  override def execute(): Unit = {

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.select($"l_orderkey", $"l_linenumber", $"l_shipdate", $"l_comment")
      .sort($"l_comment")
      .agg(count($"l_orderkey"))

    outputDF(res)

  }
}