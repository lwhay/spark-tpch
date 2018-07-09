package main.scala.cores

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Wenhai Li <lwh@whu.edu.cn>
  *
  */
class Q14pqt2 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int, url: String): Unit = {
    init(path, typeId, url)
    val res = nested.select(explode($"PartsuppList.LineitemList"), $"p_type")
      .filter($"p_name".startsWith("PROMO"))
      .select(explode($"col"), $"p_type")
      .select($"col.l_discount", $"col.l_extendedprice", $"p_type")
      .filter($"col.l_shipdate" >= "1993-11-01"
        && $"col.l_shipdate" < "1993-11-25")
      .distinct().count()
  }

}
