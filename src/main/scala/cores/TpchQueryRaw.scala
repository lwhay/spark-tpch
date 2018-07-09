package main.scala.cores

import org.apache.spark.sql._

// TPC-H table schemas
case class CustomerRaw(
                        c_custkey: Int,
                        c_name: String,
                        c_address: String,
                        c_nationkey: Int,
                        c_phone: String,
                        c_acctbal: Double,
                        c_mktsegment: String,
                        c_comment: String)

case class LineitemRaw(
                        l_orderkey: Int,
                        l_partkey: Int,
                        l_suppkey: Int,
                        l_linenumber: Int,
                        l_quantity: Double,
                        l_extendedprice: Double,
                        l_discount: Double,
                        l_tax: Double,
                        l_returnflag: String,
                        l_linestatus: String,
                        l_shipdate: String,
                        l_commitdate: String,
                        l_receiptdate: String,
                        l_shipinstruct: String,
                        l_shipmode: String,
                        l_comment: String)

case class NationRaw(
                      n_nationkey: Int,
                      n_name: String,
                      n_regionkey: Int,
                      n_comment: String)

case class OrderRaw(
                     o_orderkey: Int,
                     o_custkey: Int,
                     o_orderstatus: String,
                     o_totalprice: Double,
                     o_orderdate: String,
                     o_orderpriority: String,
                     o_clerk: String,
                     o_shippriority: Int,
                     o_comment: String)

case class PartRaw(
                    p_partkey: Int,
                    p_name: String,
                    p_mfgr: String,
                    p_brand: String,
                    p_type: String,
                    p_size: Int,
                    p_container: String,
                    p_retailprice: Double,
                    p_comment: String)

case class PartsuppRaw(
                        ps_partkey: Int,
                        ps_suppkey: Int,
                        ps_availqty: Int,
                        ps_supplycost: Double,
                        ps_comment: String)

case class RegionRaw(
                      r_regionkey: Int,
                      r_name: String,
                      r_comment: String)

case class SupplierRaw(
                        s_suppkey: Int,
                        s_name: String,
                        s_address: String,
                        s_nationkey: Int,
                        s_phone: String,
                        s_acctbal: Double,
                        s_comment: String)

/**
  * Parent class for TPC-H queries.
  *
  * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
  *
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
abstract class TpchQueryRaw {

  // read files from local FS
  // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

  // read from hdfs
  val INPUT_DIR: String = "" //"hdfs://20.20.20.31:9004/warehouse/tpch" //"/dbgen"

  // if set write results to hdfs, if null write to stdout
  // val OUTPUT_DIR: String = "/tpch"
  val OUTPUT_DIR: String = null

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  val spark = SparkSession.builder().appName("TPC-H " + className).getOrCreate()

  import spark.implicits._

  /**
    * implemented in children classes and hold the actual query
    */
  def execute(url: String): Unit = {
  }

  def outputDF(df: DataFrame): Unit = {

    if (OUTPUT_DIR == null || OUTPUT_DIR == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(OUTPUT_DIR + "/" + className + ".out") // json to avoid alias
  }
}

object TpchQueryRaw {

  /**
    * Execute query reflectively
    */
  def executeQuery(queryNo: Int, countNo: Int, url: String): Unit = {
    assert(queryNo >= 1 && queryNo <= 32, "Invalid query number")
    Class.forName(f"main.scala.cores.Q${queryNo}%02draw${countNo}%1d")
      .newInstance.asInstanceOf[ {def execute(url: String)}].execute(url)
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 3)
      executeQuery(args(0).toInt, args(1).toInt, args(2))
    else
      throw new RuntimeException("Invalid number of arguments")
  }
}
