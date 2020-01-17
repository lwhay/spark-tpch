package main.scala.tpch

import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 19
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q19 extends TpchQuery {

  import spark.implicits._

  override def execute(url: String): Unit = {
    val lineitem = spark.sparkContext.textFile(url + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
    val part = spark.sparkContext.textFile(url + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    val res = part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          sm($"p_container") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            md($"p_container") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
          (($"p_brand" === "Brand#34") &&
            lg($"p_container") &&
            $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
            $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))

    outputDF(res)

  }

}
