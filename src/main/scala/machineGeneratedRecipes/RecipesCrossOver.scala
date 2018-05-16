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

object RecipesCrossOver {
  
   // Different property classes for different vertex
    class AIRecipes()
    case class recipe(val recipeName: String, val cookTime: String, val servings: Int, val category: String) extends AIRecipes
    case class ingredient(val ingredientName: String, val quantity: Double, val measurementUnit: String, val ingredientType: String, val usedIn: String) extends AIRecipes
    case class step(val description: String, val usedIn: String) extends AIRecipes
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
     
    val sc = SparkContext.getOrCreate()  
    
    val filename = "data/recipeData.txt"
    var current = "recipe"
    var vertexArray = Array.empty[(Long, Object)]
    var edgeArray = Array.empty[Edge[edgeProperty]]
    var allGraphs = ArrayBuffer[Graph[Object,edgeProperty]]()
    
    for (line <- Source.fromFile(filename).getLines) {
        
      val arr = line.split("-")
      var testObject: AIRecipes = null
      
      // Set which class to populate 
     
      if(line == "recipe")
      {
        // Array of Graphs
        val myVertices = sc.makeRDD(vertexArray)
        val myEdges = sc.makeRDD(edgeArray)
        allGraphs += Graph(myVertices, myEdges)
        
        vertexArray = Array.empty[(Long, Object)]
        edgeArray = Array.empty[Edge[edgeProperty]]  
        current = "recipe" 
      }
      else if (line == "ingredient")
        current = "ingredient"
      else if (line == "step")
        current = "step"
      else if (line == "edge")
        current = "edge"
      else
      { 
        if (current == "ingredient")
        {
          testObject = ingredient(arr(1), arr(2).toInt, arr(3), arr(4), arr(5) )
          vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
        }
        else if (current == "step")
        {
          testObject = step(arr(1),arr(2))
          vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
        }
        else if (current == "edge")
        {
           edgeArray = edgeArray ++ Array(Edge(arr(0).toLong, arr(1).toLong, edgeProperty(arr(2), arr(3))) )
        }
        else
        {
          testObject = recipe(arr(1),arr(2),arr(3).toInt,arr(4))
          vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
        }
      }
    }
      
    for (graph <- allGraphs)
    {
      println()
      println("################################")
      println("########## New Recipe ##########")
      println("################################")
      println()
      graph.triplets.collect.foreach(println)
    }
    
    // Main Graph
    
    val filtered_vertices = allGraphs(1).vertices.filter{
    case (id, vp: ingredient) => vp.usedIn == "main"
    case (id, vp: step) => vp.usedIn == "main"
    case (id, vp: recipe) => id == 1
    case _ => false
    }
    
    val filtered_edges = allGraphs(1).edges.filter{
    case Edge(src, dst, prop: edgeProperty) => prop.usedIn == "main"
    }
    
    val extractedGraph = Graph(filtered_vertices, filtered_edges)
    
    println("####################################")
    println("# Graph: Main process of Recipe_01 #")
    println("####################################")
    
    //extractedGraph.triplets.collect.foreach(println)
    
    // Side Graph
    
    val filtered_vertices1 = allGraphs(3).vertices.filter{
    case (id, vp: ingredient) => vp.usedIn != "main"
    case (id, vp: step) => vp.usedIn != "main"
    case (id, vp: recipe) => id == 1
    case _ => false
    }
    
    val filtered_edges1 = allGraphs(3).edges.filter{
    case Edge(src, dst, prop: edgeProperty) => prop.usedIn != "main"
    }
    
    val extractedGraph1 = Graph(filtered_vertices1, filtered_edges1)
    
    println("####################################")
    println("# Graph: Side process of Recipe_01 #")
    println("####################################")
    
    //extractedGraph1.triplets.collect.foreach(println)
    
    // Finding vertex with null property 
    
    val vertexWithNullProperty = extractedGraph1.vertices.filter{
    case (id, vp) => vp == null
    }
    val vertexWithNullProperty_1 = vertexWithNullProperty.take(1)
    val vertexWithNullProperty_id = vertexWithNullProperty_1(0)._1
    
    //println(vertexWithNullProperty_id)
    
    // Find vertex with no outgoing edge in 1st recipe,  source id of extra edge 
        
    val output = extractedGraph.outDegrees.rightOuterJoin(extractedGraph.vertices).map(x => (x._1, x._2._1.getOrElse(0))).collect
    val vt = output.filter{
    case (id, vp) => vp == 0
    }
    val vertexId_receipe_1 = vt(0)._1
    
    // Find desctination id of extra adge
    
    // To use later for union
    val final_filtered_edges1 = extractedGraph1.edges.filter{
    case Edge(src, dst, prop: edgeProperty) => src != vertexWithNullProperty_id
    }
    
    val final_filtered_edges1_1 = extractedGraph1.edges.filter{
    case Edge(src, dst, prop: edgeProperty) => src == vertexWithNullProperty_id
    }
    val final_filtered_edges1_1_1 = final_filtered_edges1_1.take(1)
    val dest_id = final_filtered_edges1_1_1(0).dstId
    
    // Now make a new edge
    
    val extraEdges = sc.makeRDD(Array(Edge(vertexId_receipe_1, dest_id, edgeProperty("step","main"))  ))
    
  
    // Next Step: Combine these two recipes
    
    val totalEdges = final_filtered_edges1.union(extraEdges)  
    val newgraph = Graph( extractedGraph.vertices.union(extractedGraph1.vertices), extractedGraph.edges.union(totalEdges)) 
    
    println("###################################")
    println("######  Graph: New Receipe  #######")
    println("###################################")
    
    //newgraph.triplets.collect.foreach(println)
   
    spark.stop
  }
}
  

