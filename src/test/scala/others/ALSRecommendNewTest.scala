package others

import java.util.Calendar

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object ALSRecommendNewTest {

  case class MVRating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): MVRating = {
    val fields = str.split("[\t ]+")
    assert(fields.length == 4)
    MVRating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .master("local[6]")     // 本机有8个逻辑核心，由于有其它程序，故只能小于8
        .appName("ALSExample")
      .getOrCreate()

    import spark.implicits._

    val start = Calendar.getInstance().getTimeInMillis
    val ratings = spark.read.textFile("./ml-100k/u.data")
      .map(parseRating)
      .repartition(6)   // 每个分区对应一个任务
      .toDF()

//    ratings.printSchema()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(10)
      .setRank(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)

    //  忽略未评分的记录
    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)

    predictions.cache()

    println(predictions.count())
    predictions.show(10)
    val end = Calendar.getInstance().getTimeInMillis
    println(s"tatol cost: ${end - start}")

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    spark.stop()
  }

}
