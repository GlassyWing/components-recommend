package org.manlier.recommend

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.manlier.recommend.common.SimilarityMeasures._
import org.manlier.recommend.entities.{History, UserCompPair}

object LinearItemCFModel {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .config(new SparkConf())
      .getOrCreate()

    import spark.implicits._

    val recommender = new LinearItemCFModel(spark)

    val history = spark.read.csv(args(0))
      .map(row => History(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toInt, row.getString(3).toFloat))
      .coalesce(spark.sparkContext.defaultParallelism)
      .cache()

    recommender.fit(history)

    showSimilarities(recommender)
    testRecommend(recommender, spark, args(1).toInt, args(2).toInt)
  }

  private def showSimilarities(recommender: LinearItemCFModel): Unit = {
    val start = Calendar.getInstance().getTimeInMillis
    recommender.getSimilarities.get
      .sort("compId", "followCompId1", "followCompId2", "cooc")
      .show()
    val end = Calendar.getInstance().getTimeInMillis
    println(s"Total cost: ${(end - start) / 1000f} s")
  }

  private def testRecommend(recommender: LinearItemCFModel, spark: SparkSession, userId: Int, compId: Int): Unit = {
    import spark.implicits._
    val users = spark.createDataset(Seq(UserCompPair(userId, compId)))
    val start = Calendar.getInstance().getTimeInMillis
    recommender.recommendForUser(users, 3).show(truncate = false)
    val end = Calendar.getInstance().getTimeInMillis
    println(s"Total cost: ${(end - start) / 1000f} s")
  }
}

/**
  * 物品间有先后关系的基于物品的协同过滤模型
  *
  * @param spark              SparkSession  对象
  * @param similarityMeasure  相似度度量算法，支持的算法有
  *                           - cooc: 同现相似度
  */
class LinearItemCFModel(spark: SparkSession, similarityMeasure: String) extends Serializable {

  def this(spark: SparkSession) = this(spark, "cooc")

  val defaultParallelism: Int = spark.sparkContext.defaultParallelism

  var history: Option[Dataset[History]] = None
  var similarities: Option[DataFrame] = None

  def getSimilarities: Option[DataFrame] = similarities

  import spark.implicits._

  /**
    * 使用数据进行训练
    *
    * @param history 用户使用历史
    * @return LinearItemCFModel
    */
  def fit(history: Dataset[History]): LinearItemCFModel = {

    this.history = Option(history)

    //  得到使用了第一个构件又使用了第二个构件的用户数量
    val numRaters = history.groupBy("compId", "followCompId").count()
      .toDF("compId", "followCompId", "numRaters")
      .coalesce(defaultParallelism)

    // 将表numRaters和表history进行内联操作，并忽略掉count
    val historyWithSize = history.join(numRaters, Seq("compId", "followCompId"))
      .select("userId", "compId", "followCompId", "numRaters")
      .coalesce(defaultParallelism)

    //  内联获得所有comp可用的后续构件，并按照followCompId1 <= followCompId2过滤掉重复的行：
    historyWithSize.join(historyWithSize, Seq("userId", "compId"))
      .toDF("userId", "compId", "followCompId1", "numRaters1", "followCompId2", "numRaters2")
      .where($"followCompId1" <= $"followCompId2")
      .coalesce(defaultParallelism)
      .createOrReplaceTempView("joined")

    //  计算使用完comp后既使用过followComp1又使用过followComp2的用户数，并记为size
    val sparseMatrix = spark.sql(
      """
        | SELECT compId
        |, followCompId1
        |, followCompId2
        |, count(userId) as size
        |, first(numRaters1) as numRaters1
        |, first(numRaters2) as numRaters2
        | FROM joined
        | GROUP BY compId, followCompId1, followCompId2
      """.stripMargin)
      .coalesce(defaultParallelism)

    // 计算相似度
    val sim = sparseMatrix.map(row => {
      val compId = row.getInt(0)
      val followCompId1 = row.getInt(1)
      val followCompId2 = row.getInt(2)
      val size = row.getLong(3)
      val numRaters1 = row.getLong(4)
      val numRaters2 = row.getLong(5)

      val cooc = cooccurrence(size, numRaters1, numRaters2)
      (compId, followCompId1, followCompId2, cooc)
    }).toDF("compId", "followCompId1", "followCompId2", "cooc")
      .coalesce(defaultParallelism)

    similarities = Option(reverseSimilaritiesAndUnion(sim))
    this
  }

  def recommendForUser(users: Dataset[UserCompPair], num: Int): DataFrame = {

    val history = this.history.get

    val sim = similarities.get
      .select("compId", "followCompId1", "followCompId2", similarityMeasure)

    val project = users
      //  进行子投影
      .join(history, Seq("userId", "compId"))
      .selectExpr("userId", "compId", "followCompId as followCompId1", "count")
      .coalesce(defaultParallelism)

    // 获得用户感兴趣的物品与其它物品的相似度
    project.join(sim, Seq("compId", "followCompId1"))
      .selectExpr("userId"
        , "compId"
        , "followCompId1"
        , "followCompId2"
        , similarityMeasure
        , s"$similarityMeasure * count as simProduct")
      .coalesce(defaultParallelism)
      .createOrReplaceTempView("tempView")

    spark.sql(
      s"""
         |SELECT userId
         |,  compId
         |,  followCompId2 as otherCompId
         |,  sum(simProduct) / sum($similarityMeasure) as  count
         |FROM tempView
         |GROUP BY userId, compId, followCompId2
         |ORDER BY userId asc, count desc
      """.stripMargin)
      .rdd
      .map(row => ((row.getInt(0), row.getInt(1)), (row.getInt(2), row.getDouble(3))))
      .groupByKey()
      .coalesce(defaultParallelism)
      .mapValues(xs => {
        var sequence = Seq[(Int, Double)]()
        val iter = xs.iterator
        var count = 0
        while (iter.hasNext && count < num) {
          val rat = iter.next()
          if (rat._2 != Double.NaN)
            sequence :+= (rat._1, rat._2)
          count += 1
        }
        sequence
      })
      .flatMap(xs => xs._2.map(xs2 => (xs._1._1, xs._1._2, xs2._1, xs2._2)))
      .toDF("userId", "compId", "followCompId", "prediction")
  }

  private def reverseSimilaritiesAndUnion(sim: DataFrame): DataFrame = {
    sim.cache()
    val finalSim = sim.map(row => {
      (row.getInt(0), row.getInt(2), row.getInt(1), row.getDouble(3))
    }).toDF("compId", "followCompId1", "followCompId2", "cooc")
      .union(sim)
      .coalesce(defaultParallelism)
      .cache()
    finalSim.count()  //  缓存最终的相似度
    sim.unpersist()
    finalSim
  }

}
