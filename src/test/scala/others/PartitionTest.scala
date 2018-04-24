package others

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class PartitionTest extends FunSuite with BeforeAndAfterEach {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("partition test")
    .getOrCreate()

  var numbersDF: DataFrame = _

  import spark.implicits._

  override protected def beforeEach(): Unit = {
    val x = (1 to 10).toList
    numbersDF = x.toDF()
  }

  override protected def afterEach(): Unit = {
    spark.stop()
  }

  test("intro") {
    println(numbersDF.rdd.partitions.length)
    numbersDF.write.csv("./output")
  }

  test("coalesce") {
    val numbersDf2 = numbersDF.coalesce(2)
    println(numbersDf2.rdd.partitions.length)
    numbersDf2.write.csv("./output")
  }

  test("increasing partitions by coalesce") {
    val numbersDf3 = numbersDF.coalesce(6)
    assert(numbersDf3.rdd.partitions.length === 4)
  }

  test("increasing partitions by repartition") {
    // repartition算子会执行完整的数据混洗并在分区之间平均分配数据。它不会像coalesce算法那样尽量减少数据移动。
    val homerDf = numbersDF.repartition(2)
    assert(homerDf.rdd.partitions.length === 2)
    homerDf.write.csv("./output")
  }

  test("repartition by column") {
    val people = List(
      (10, "blue"),
      (13, "red"),
      (15, "blue"),
      (99, "red"),
      (67, "blue")
    )
    val peopleDf = people.toDF("age", "color")
    val colorDf = peopleDf.repartition($"color")
    colorDf.write.csv("./output")
  }

}
