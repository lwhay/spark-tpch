package main.scala.cores

import org.apache.spark.sql.functions._

/**
  * TPC-H Query 6
  * Wenhai Li <lwh@whu.edu.cn>
  *
  */
class Q19pqt3 extends TpchQueryPqt {

  import spark.implicits._

  override def execute(path: String, typeId: Int, url: String): Unit = {
    init(path, typeId, url)
    val sm = udf { (x: String) => x.matches("SM|MED") }

    val res = nested.select(explode($"PartsuppList.LineitemList"))
      .filter(
        (($"p_brand" === "Brand#50") &&
          sm($"p_container") && $"p_size" <= 20))
      .select(explode($"col"))
      .select($"col.l_discount", $"col.l_extendedprice")
      .filter(($"col.l_shipmode" === "TRUCK" || $"col.l_shipmode" === "AIR") &&
        $"col.l_shipinstruct" === "DELIVER IN PERSON" && $"col.l_quantity" >= 10 && $"col.l_quantity" <= 30)
      .select($"l_extendedprice", $"l_discount").count()
  }

}
