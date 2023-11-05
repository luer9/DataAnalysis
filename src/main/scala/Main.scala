import org.apache.spark.SparkContext
import java.nio.file.{Path, Paths}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory, Statement}
import org.apache.jena.vocabulary.RDF
object Main {
  val spark: SparkSession = Configuration.Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val inputTriFile = "F:\\dbproject\\NCI_MAPPING\\data\\dbpedia\\dbpedia.tri"
    val inputPredFile = "F:\\dbproject\\NCI_MAPPING\\data\\dbpedia\\dbpedia.p"
//    val outputDIR = "F:\\dbproject\\DataAnaysis\\VP"
    // 得到vp表 ==========================
    //id数据集url
    val triDF = DataProcess.DataReader.getTriples(inputTriFile)
//    val predsDF = DataProcess.DataReader.getPreds(inputPredFile)
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

    // id 转 url

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
