import org.apache.spark.SparkContext

import java.nio.file.{Path, Paths}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, PrintWriter}

object Main {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val inputTriFile = "F:\\dbproject\\NCI_MAPPING\\data\\dbpedia\\dbpedia.tri"
    val inputPredFile = "F:\\dbproject\\NCI_MAPPING\\data\\dbpedia\\dbpedia.p"
    val inputSoFile = "F:\\dbproject\\NCI_MAPPING\\data\\dbpedia\\dbpedia.so"
//    val inputTriFile = "/home/lulu/new/dbpedia/dbpedia.tri"
//    val inputPredFile = "/home/lulu/new/dbpedia/dbpedia.p"
//    val inputSoFile = "/home/lulu/new/dbpedia/dbpedia.so"
//    val outputDIR = "F:\\dbproject\\DataAnaysis\\VP"
    // 得到vp表 ==========================
    //id数据集url
    val triDF = DataProcess.DataReader.getTriples(inputTriFile)
    val predsDF = DataProcess.DataReader.getPreds(inputPredFile)
    val sosDF = DataProcess.DataReader.getSos(inputSoFile)
//    println("pred count: " + predsDF.count()) // 60615
    // vp 分表
    // =============================
//    predsDF.select("predID").collect().foreach(pred => {
//      println("---> " + pred.get(0))
//      triDF.select("sub", "obj").where($"pred" === pred.get(0))
//        .write.parquet(outputDIR + File.separator + pred.get(0))
//    })
    // ==========================

    // 读取excel表格 筛选三元组
//    val excelfile1 = "F:\\dbproject\\dbpediaAn\\short23.xlsx"
//    val excelfile2 = "F:\\dbproject\\dbpediaAn\\long456.xlsx"
//    val excelfile3 = "F:\\dbproject\\dbpediaAn\\long789.xlsx"
//    val excelfile4 = "F:\\dbproject\\dbpediaAn\\long101112.xlsx"
//    val excelfile5 = "F:\\dbproject\\dbpediaAn\\long131415.xlsx"
//    val excelfile6 = "F:\\dbproject\\dbpediaAn\\long161718.xlsx"
//    val excelfile7 = "F:\\dbproject\\dbpediaAn\\long192021.xlsx"
//    val excelfile8 = "F:\\dbproject\\dbpediaAn\\long22plus.xlsx"
//    val outputDIR = "F:\\dbproject\\DataAnaysis\\data"
//    filesplit(triDF, excelfile2, outputDIR)
//    filesplit(triDF, excelfile3, outputDIR)
//    filesplit(triDF, excelfile4, outputDIR)
//    filesplit(triDF, excelfile5, outputDIR)
//    filesplit(triDF, excelfile6, outputDIR)
//    filesplit(triDF, excelfile7, outputDIR)
//    filesplit(triDF, excelfile8, outputDIR)

//    val predFile = "F:\\dbproject\\DataAnaysis\\data\\1243.txt"
//    val outfile = "F:\\dbproject\\DataAnaysis\\data\\dbpediaIndex" // dbpedia 的 id 索引版
//    dataBypred(triDF, predFile, outfile)

    // dbepdia parquet -> txt
//    triDF.rdd.map(_.toString()).saveAsTextFile("F:\\dbproject\\DataAnaysis\\data\\dbpediaCom" )

    // id 转 url
    // ---------------dbpediaIndex
    // ----- 输入 parquet 文件，输出dbpediaIndex sos映射文件，pred映射文件
//    val dbpediaIndex = "/home/lulu/new/dbpedia/dbpediaIndex"
//    val outputFile = "/home/lulu/new/dbpedia/dbpediaIndexURL"
//    idToURL(triDF, sosDF, predsDF, dbpediaIndex, outputFile)
    // 生成查询。
    // ----- 得到谓词映射生成查询
    // ---------得到每个excel下的谓词，
    // ------------ 做谓词映射
    // ---------------- 生成查询保存。
////    val excelfile1 = "F:\\dbproject\\dbpediaAn\\short23.xlsx"
//    val excelfile2 = "F:\\dbproject\\dbpediaAn\\long456.xlsx"
//    val excelfile3 = "F:\\dbproject\\dbpediaAn\\long789.xlsx"
//    val excelfile4 = "F:\\dbproject\\dbpediaAn\\long101112.xlsx"
//    val excelfile5 = "F:\\dbproject\\dbpediaAn\\long131415.xlsx"
//    val excelfile6 = "F:\\dbproject\\dbpediaAn\\long161718.xlsx"
//    val excelfile7 = "F:\\dbproject\\dbpediaAn\\long192021.xlsx"
//    val excelfile8 = "F:\\dbproject\\dbpediaAn\\long22plus.xlsx"
//    val outputDIR = "F:\\dbproject\\DataAnaysis\\query"
////    getQueryByPred(predsDF, excelfile1, outputDIR)
//    getQueryByPred(predsDF, excelfile2, outputDIR)
//    getQueryByPred(predsDF, excelfile3, outputDIR)
//    getQueryByPred(predsDF, excelfile4, outputDIR)
//    getQueryByPred(predsDF, excelfile5, outputDIR)
//    getQueryByPred(predsDF, excelfile6, outputDIR)
//    getQueryByPred(predsDF, excelfile7, outputDIR)
//    getQueryByPred(predsDF, excelfile8, outputDIR)

    val excelfile1 = "F:\\dbproject\\dbpediaAn\\short23.xlsx"
    val excelfile2 = "F:\\dbproject\\dbpediaAn\\long456.xlsx"
    val excelfile3 = "F:\\dbproject\\dbpediaAn\\long789.xlsx"
    val excelfile4 = "F:\\dbproject\\dbpediaAn\\long101112.xlsx"
    val excelfile5 = "F:\\dbproject\\dbpediaAn\\long131415.xlsx"
    val excelfile6 = "F:\\dbproject\\dbpediaAn\\long161718.xlsx"
    val excelfile7 = "F:\\dbproject\\dbpediaAn\\long192021.xlsx"
    val excelfile8 = "F:\\dbproject\\dbpediaAn\\long22plus.xlsx"
    val outputDIR = "F:\\dbproject\\DataAnaysis\\queryID"
    getQueryIDByPred(predsDF, excelfile1, outputDIR)
    getQueryIDByPred(predsDF, excelfile2, outputDIR)
    getQueryIDByPred(predsDF, excelfile3, outputDIR)
    getQueryIDByPred(predsDF, excelfile4, outputDIR)
    getQueryIDByPred(predsDF, excelfile5, outputDIR)
    getQueryIDByPred(predsDF, excelfile6, outputDIR)
    getQueryIDByPred(predsDF, excelfile7, outputDIR)
    getQueryIDByPred(predsDF, excelfile8, outputDIR)
  }
  def idToURL(triDF: DataFrame, soDF: DataFrame, predDF: DataFrame, inputfile: String, output: String): Unit = {

    val dbpediaIndex = spark.read
      .parquet(inputfile)
      .withColumnRenamed("sub", "sub1")
      .withColumnRenamed("obj", "obj1")
      .withColumnRenamed("pred", "pred1")
      .toDF()
    dbpediaIndex.show()
    val temp1 = dbpediaIndex
      .join(predDF, dbpediaIndex("pred1") === predDF("predID"), "inner")
      .toDF()
    println("temp1:")
    temp1.show()
    val temp2 = temp1
      .join(soDF, soDF("sosID") === dbpediaIndex("sub1"), "inner")
      .withColumnRenamed("sos", "sub")
      .toDF()
    temp1.unpersist()
    println("temp2:")
    temp2.show()
    val temp3 = temp2
      .join(soDF, soDF("sosID") === dbpediaIndex("obj1"), "inner")
      .withColumnRenamed("sos", "obj")
      .select("sub", "pred", "obj").distinct()
      .toDF()
    temp2.unpersist()
    println("temp3:" + temp3.count())
    temp3.show()
    temp3.write.parquet(output)
    println("parquet done ")
    temp3.rdd.map(_.toString()).saveAsTextFile(output + "TXT" )
    println("TXT done ")
    temp3.unpersist()
  }
  def dataBypred(triDF: DataFrame,file: String, outfile: String): Unit = {
    // 筛选指定pred的三元组
    val pred = spark.read.textFile(file).withColumnRenamed("value", "pred").toDF()
    pred.show()
    val res = triDF.join(pred, Seq("pred"), "inner").select("sub", "pred", "obj").distinct().toDF
    //    val edges = res.count()
//    val nodes = res.select("sub").union(res.select("obj")).distinct().toDF().count()
    //    println("边：" + edges)
    //    println("节点：" + nodes)
    res.write.parquet(outfile)
  }


  def getQueryIDByPred(predDF: DataFrame, excelPath: String, outpath: String): Unit = {
    val table = spark.read.format("com.crealytics.spark.excel")
      .option("inferSchema", "true")
      .option("useHeader", "true")
      .load(excelPath).select("pred").withColumnRenamed("pred", "pred1").distinct().toDF()
    val fileName = Paths.get(excelPath).getFileName.toString
    val writer = new PrintWriter(outpath + File.separator + fileName.substring(0, fileName.lastIndexOf('.')))
    table.collect().map(row => {
      writer.println(query("<"+row.get(0).toString+">"))
    })
    writer.flush()
    writer.close()
    println("文件保存成功!")
  }
  def getQueryByPred(predDF: DataFrame, excelPath: String, outpath: String): Unit = {
    val table = spark.read.format("com.crealytics.spark.excel")
      .option("inferSchema", "true")
      .option("useHeader", "true")
      .load(excelPath).select("pred").withColumnRenamed("pred", "pred1").distinct().toDF()
    val fileName = Paths.get(excelPath).getFileName.toString
    val frame = predDF.join(table, predDF("predID") === table("pred1"), "inner").select("pred").distinct().toDF()
    val writer = new PrintWriter(outpath + File.separator + fileName.substring(0, fileName.lastIndexOf('.')))
    frame.collect().map(row => {
      writer.println(query(row.get(0).toString))
    })
    writer.flush()
    writer.close()
    println("文件保存成功!")
  }
  def query(pred: String): String = {
    "SELECT ?X ?Y WHERE {  ?X   " + pred + "*" + "   ?Y }"
  }
  def filesplit(triDF: DataFrame, path: String, outpath: String): Unit = {
    val table = spark.read.format("com.crealytics.spark.excel")
      .option("inferSchema", "true")
      .option("useHeader", "true")
      .load(path).select("pred").toDF()
    // 文件名 原文件名-谓词数-三元组数
    val res = triDF.join(table, Seq("pred"), "inner").select("sub", "pred", "obj").distinct().toDF
    val fileName = Paths.get(path).getFileName.toString
    val preds = table.distinct().count()
    val tris = res.count()
    println("文件名：" + fileName.substring(0, fileName.lastIndexOf('.')))
    println("谓词数：" + preds)
    println("三元组数：" + tris)
    val totaln = fileName.substring(0, fileName.lastIndexOf('.')) + "-" + preds + "-" + tris
    res.write.parquet(outpath + File.separator + totaln)
    println(totaln + ": 完成=======")
  }
}
