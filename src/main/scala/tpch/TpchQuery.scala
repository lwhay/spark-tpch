package main.scala.tpch

import java.io.File
import java.net._
import org.apache.spark.sql._

// TPC-H table schemas
case class Customer(
                     c_custkey: Long,
                     c_name: String,
                     c_address: String,
                     c_nationkey: Long,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Lineitem(
                     l_orderkey: Long,
                     l_partkey: Long,
                     l_suppkey: Long,
                     l_linenumber: Long,
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

case class Nation(
                   n_nationkey: Long,
                   n_name: String,
                   n_regionkey: Long,
                   n_comment: String)

case class Order(
                  o_orderkey: Long,
                  o_custkey: Long,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Long,
                  o_comment: String)

case class Part(
                 p_partkey: Long,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Long,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Long,
                     ps_suppkey: Long,
                     ps_availqty: Long,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Long,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Long,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Long,
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
abstract class TpchQuery {

  // read files from local FS
  // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

  // read from hdfs
  val localhost: InetAddress = InetAddress.getLocalHost
  val localIpAddress: String = localhost.getHostAddress
  val INPUT_DIR: String = "hdfs://" + localIpAddress + ":9004/warehouse/tpch" //"/dbgen"
  //val INPUT_DIR: String = "hdfs://172.16.2.209:9004/warehouse/tpch" //"/dbgen"

  // if set write results to hdfs, if null write to stdout
  // val OUTPUT_DIR: String = "/tpch"
  val OUTPUT_DIR: String = null

  // get the name of the class excluding dollar signs and package
  //val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  val spark = SparkSession.builder().appName("TPC-H " /*+ className*/).getOrCreate()

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
      df.write.mode("overwrite").json(OUTPUT_DIR + "/" /*+ className*/ + "query.out") // json to avoid alias
  }
}

object TpchQuery {

  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int, url: String): Unit = {
    assert(queryNo >= 1 && queryNo <= 32, "Invalid query number")
    Class.forName(f"main.scala.tpch.Q${queryNo}%02d").newInstance.asInstanceOf[ {def execute(url: String)}].execute(url)
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 2)
      executeQuery(args(0).toInt, args(1))
    else
      throw new RuntimeException("Invalid number of arguments")
  }
}
