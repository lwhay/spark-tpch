package main.scala

import org.apache.spark.sql.functions._

/**
  * Created by michael on 1/3/17.
  */
class Q28 extends TpchQuery {

  import spark.implicits._

  override def execute(): Unit = {

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.select($"l_orderkey", $"l_extendedprice", $"l_linenumber", $"l_comment")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"o_orderdate", $"l_extendedprice", $"l_orderkey", $"l_linenumber", $"l_comment")
      .sort($"l_extendedprice")
      .agg(count($"l_orderkey"))

    outputDF(res)

  }

}
