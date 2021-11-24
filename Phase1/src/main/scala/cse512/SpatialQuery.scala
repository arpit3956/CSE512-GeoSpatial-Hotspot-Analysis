package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def STContains = (queryRectangle:String, pointString:String) => {
    val str_rect = queryRectangle.split(",")
    val str_point = pointString.split(",")
    val rect = new Array[Double](str_rect.length)
    val point = new Array[Double](str_point.length)
    for (i <- rect.indices) {
      rect(i) = str_rect(i).toDouble
    }
    for (i <- point.indices) {
      point(i) = str_point(i).toDouble
    }
    if (rect(0)<=point(0) && point(0)<=rect(2) && rect(1)<=point(1) && point(1)<=rect(3)) true else false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", STContains)

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", STContains)

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def STWithin = (pointString1:String, pointString2:String, distance:Double) => {
    val str_point1 = pointString1.split(",")
    val str_point2 = pointString2.split(",")
    val point1 = new Array[Double](str_point1.length)
    val point2 = new Array[Double](str_point2.length)
    for (i <- point1.indices) {
      point1(i) = str_point1(i).toDouble
    }
    for (i <- point2.indices) {
      point2(i) = str_point2(i).toDouble
    }
    val x = point1(0)-point2(0)
    val y = point1(1)-point2(1)
    if (math.sqrt(x*x + y*y) < distance) true else false
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",STWithin)

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", STWithin)
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
