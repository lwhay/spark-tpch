package main.scala.cores

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Wenhai Li <lwh@whu.edu.cn>
  *
  */
class Q06pqt3 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int, url: String): Unit = {
    init(path, typeId, url)
    val res = nested.select(explode($"PartsuppList.LineitemList"))
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_shipdate", $"col.l_quantity", $"col.l_extendedprice")
      .filter($"col.l_shipdate" >= "1993-10-10"
        && $"col.l_shipdate" < "1994-01-01"
        && $"col.l_discount" >= 0.03
        && $"col.l_discount" <= 0.04
        && $"col.l_quantity" < 9)
      .agg(sum($"l_extendedprice" * $"l_discount"))
    outputDF(res)
  }

}
