package main.scala

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
  * Created by michael on 12/15/16.
  */
class Q24 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    val res = lineitem.select($"l_orderkey", $"l_linenumber", $"l_suppkey", $"l_partkey", $"l_quantity", $"l_commitdate", $"l_receiptdate", $"l_comment")
      .sort($"l_extendedprice")
      .agg(count($"l_orderkey"))

    outputDF(res)

  }
}