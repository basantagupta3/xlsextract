package com.home.xls

import java.io.File
import java.io._
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.poi.ss.usermodel.Workbook

import scala.collection.mutable.ListBuffer

object DataExtractor {

  case class ProductYield(country: String, item: String, year: String, production: String)

  def main(args: Array[String]): Unit = {

    val path = getClass.getResource("/data/InternationalBaseline2019-Final.xlsx")
    val myFile = new File(path.getPath)
    val workBook = WorkbookFactory.create(myFile);
    var prodList = new ListBuffer[ProductYield]()
    val countryList = Seq("USA", "WORLD")
    var sb=new StringBuffer()
    sb.append("country"+","+"item"+","+"year"+","+"production"+"\n")
    for(country <- countryList){
      prodList = prodList ++ getFormattedDataForCountry(country,sb,workBook)
    }

    println(sb)
    val outpath = getClass.getResource("/data")
    write(sb.toString,outpath.getPath+"/formatted_data.csv")

  }


  def getFormattedDataForCountry(country: String,sb:StringBuffer,workBook:Workbook): ListBuffer[ProductYield] = {

    val itemsMap = Map("Barley" -> 1, "Beef" -> 2, "Corn" -> 3,
      "Cotton" -> 4, "Pork" -> 5,"Poultry" -> 6, "Rice" -> 7, "Sorghum" -> 8, "Soybeans" -> 9,
      "Soybean meal" -> 10, "Soybean oil" -> 11, "Wheat" -> 12
    )
    var prodList = new ListBuffer[ProductYield]()

    for ((k, v) <- itemsMap) {


      val _sheet = workBook.getSheetAt(v)

      val rows = _sheet.iterator()
      while (rows.hasNext) {
        val row = rows.next()
        var currentRow = row.getRowNum
        var cells = row.iterator()
        val line=cells.next().toString

        if (line.contains(country+" ")) {
          val start = currentRow + 3
          val end = currentRow + 15
          for (index <- start  to end) {
            val argRowStart = _sheet.getRow(index)
            if(k == "Poultry"){
              val item = ProductYield(country, k, argRowStart.getCell(0).toString,
                argRowStart.getCell(1).toString)
              sb.append(country+","+k+","+formattedDate(argRowStart.getCell(0).toString.trim)+","+argRowStart.getCell(1).toString.trim+"\n")
              prodList += item
            }else{
              val item = ProductYield(country, k, argRowStart.getCell(0).toString,
                argRowStart.getCell(3).toString)
              sb.append(country+","+k+","+formattedDate(argRowStart.getCell(0).toString.trim)+","+argRowStart.getCell(3).toString.trim+"\n")
              prodList += item
            }

          }

        }

      }


    }

    prodList

  }

  def write(str:String, file:String):Unit = {
    val writer = new PrintWriter(new File(file))
    writer.write(str)
    writer.close()
  }

  def formattedDate(str:String):String = {
    if(str.contains("/")){
      str.split("/")(0)
    }else {
      str.split("\\.")(0)
    }
  }


}

