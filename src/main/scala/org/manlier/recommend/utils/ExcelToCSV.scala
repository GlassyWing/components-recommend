package org.manlier.recommend.utils

import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.poifs.filesystem.OfficeXmlFileException
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.manlier.recommend.entities.{Component, History, User}

import scala.collection.JavaConverters._

object ExcelToCSV {

  val numUsers = 10

  def getUserId: Int = {
    synchronized {
      val ori = math.random * numUsers
      math.ceil(ori).toInt
    }
  }

  def generatCSV(excelFilePath: String, outputDir: String): Unit = {
    var workbook: Option[ {def getSheetAt(int: Int): Sheet}] = None
    try {
      workbook = Some(new HSSFWorkbook(new FileInputStream(excelFilePath)))
    } catch {
      case _: OfficeXmlFileException =>
        workbook = Some(new XSSFWorkbook(new FileInputStream(excelFilePath)))
    }

    val sheet: Sheet = workbook.get.getSheetAt(0)
    val rowIter = sheet.rowIterator()
    rowIter.next()

    var records: ConcurrentLinkedQueue[History] = new ConcurrentLinkedQueue[History]()
    val users: ConcurrentHashMap[String, Int] = new ConcurrentHashMap[String, Int]()
    val comps: ConcurrentHashMap[String, Int] = new ConcurrentHashMap[String, Int]()
    val userCount: AtomicInteger = new AtomicInteger(0)
    val compCount: AtomicInteger = new AtomicInteger(0)

    rowIter.asScala.toSeq.par.foreach(row => {
      //      val userName: String = row.getCell(0).getNumericCellValue.toString
      val userId = getUserId
      val userName: String = userId.toString
      val compName: String = row.getCell(1).getStringCellValue
      val refCompName: String = row.getCell(2).getStringCellValue
      val freq: Long = row.getCell(3).getNumericCellValue.toLong

      print(userName + " ")

      if (!users.contains(userName)) {
        users.put(userName, userId)
        userCount.getAndAdd(1)
      }
      if (!comps.contains(compName)) {
        comps.put(compName, compCount.getAndAdd(1))
      }
      if (!comps.contains(refCompName)) {
        comps.put(refCompName, compCount.getAndAdd(1))
      }

      records.add(History(users.get(userName), comps.get(compName), comps.get(refCompName), freq))
    })

    println(records.size())

    val spark = SparkSession.builder()
      .master("local")
      .appName("converter")
      .getOrCreate()

    import spark.implicits._

    records.asScala.toSeq.sortBy(h => h.userId)
      .toDF("userId", "compId", "refCompId", "freq").write
      .mode(SaveMode.Overwrite)
      .csv(outputDir + "/history")
    users.asScala.map({ case (name, id) => User(id, name) })
      .seq.toSeq.sortBy(u => u.userId)
      .toDF("id", "name")
      .write.mode(SaveMode.Overwrite).csv(outputDir + "/users")

    comps.asScala.map({ case (name, id) => Component(id, name) })
      .seq.toSeq.sortBy(c => c.compId)
      .toDF("id", "name")
      .write.mode(SaveMode.Overwrite).csv(outputDir + "/components")
  }

  def main(args: Array[String]): Unit = {
    generatCSV(args(0), args(1))
  }

}
