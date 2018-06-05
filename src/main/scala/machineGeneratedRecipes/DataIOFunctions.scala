package machineGeneratedRecipes
import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

//import better.files._
//import better.files.File._

import java.util._
import java.io.File

import scala.collection.mutable
import scala.io.Source
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.graphframes._
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object DataIOFunctions {
  
  // Delet file
  
   def deleteFile(path: String) = {
    val fileTemp = new File(path)
    if (fileTemp.exists) {
       fileTemp.delete()
    }
  }
}
  

