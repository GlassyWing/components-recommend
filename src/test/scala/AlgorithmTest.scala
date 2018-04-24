import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class AlgorithmTest extends FunSuite with BeforeAndAfterEach {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  test("join") {
    val s = spark.createDataset(Array((1, 1, "A"), (2, 2, "B"), (3, 3, "C")))
      .toDF("id", "id2", "name")
    s.join(s, "id").show()
  }

}
