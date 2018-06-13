package machineGeneratedRecipes
import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.io.Source
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.graphframes._
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object RecipesCrossOver {
  
   // Different property classes for different vertex
  
  // textute_mainIngredient (soft, hard)
  // cookedType_mainIngredient (notCooked, halfCooked, cooked)
  
  sealed trait AIRecipes 
  case class recipe(val recipeName: String, val cookTime: String, val servings: Int, val category: String, val sideProcess: Int, val cookedType_mainIngredient: String) extends AIRecipes
  case class ingredient(val ingredientName: String, val quantity: Float, val measurementUnit: String, val ingredientType: String, val usedIn: String) extends AIRecipes
  case class step(val description: String, val stepNo: Int, val usedIn: String) extends AIRecipes
  case class edgeProperty(val relation: String, val usedIn: String) extends AIRecipes
    
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName(s"Machine Generated Recipes")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
      

    println("###########################################")
    println("######   Machine Generated Recipes   ######")
    println("###########################################")
    
    // Notes on Data: Remove index 3,4,6, & 7. Consider 0-15 other than that. 
     
    val sc = SparkContext.getOrCreate()  
    val filename = "data/recipeData-New.txt"
    var allGraphs = GraphToDescription.readAllrecipesFromFileToGraph(filename)
    
    DataIOFunctions.deleteFile("data/recipesOutput.txt")
    scala.tools.nsc.io.Path("data/recipesOutput.txt").createFile()
    
    var recipeCount = 1
    
    for (index1 <- 0 to allGraphs.length-1){
      for (index2 <- 0 to allGraphs.length-1){
        println(recipeCount)
        if(index1 != index2 && index1 != 3 && index1 != 4 && index1 != 6 && index1 != 7 && index2 != 3 && index2 != 4 && index2 != 6 && index2 != 7){
    
          if( GraphToDescription.isCompatible(allGraphs,index1,index2)) {
            scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll("\n\n#######  Receipe  " + recipeCount + " #######\n\n")
            val newgraph = GraphToDescription.swapTworecipes(allGraphs,index1,index2)
            GraphToDescription.gToDescription(newgraph, allGraphs,index1,index2)
            recipeCount = recipeCount + 1 
          }
          else{
            println("Not Comaptible.")
          }
    
        }
      }
    }
    
    println("Execution finished here.")
    
    spark.stop
  }
}  

