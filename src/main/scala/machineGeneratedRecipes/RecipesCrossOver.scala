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
    //class AIRecipes()
    sealed trait AIRecipes
    case class recipe(val recipeName: String, val cookTime: String, val servings: Int, val category: String, val sideProcess: Int) extends AIRecipes
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
     
    val sc = SparkContext.getOrCreate()  
    
    val filename = "data/recipeData-New.txt"
    var current = "recipe"
    var vertexArray = Array.empty[(Long, AIRecipes)]
    var edgeArray = Array.empty[Edge[edgeProperty]]
    var allGraphs = ArrayBuffer[Graph[AIRecipes,edgeProperty]]()
    
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
        
        vertexArray = Array.empty[(Long, AIRecipes)]
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
          testObject = ingredient(arr(1), arr(2).toFloat, arr(3), arr(4), arr(5) )
          vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
        }
        else if (current == "step")
        {
          testObject = step(arr(1),arr(2).toInt,arr(3))
          vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
        }
        else if (current == "edge")
        {
           edgeArray = edgeArray ++ Array(Edge(arr(0).toLong, arr(1).toLong, edgeProperty(arr(2), arr(3))) )
        }
        else
        {
          testObject = recipe(arr(1),arr(2),arr(3).toInt,arr(4),arr(5).toInt)
          vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
        }
      }
    }
      
    /*for (graph <- allGraphs)
    {
      println()
      println("################################")
      println("########## New Recipe ##########")
      println("################################")
      println()
      graph.triplets.collect.foreach(println)
    }*/
    
    println("####################################")
    println("# Graph: Main process of Recipe_01 #")
    println("####################################")
    
    val filtered_vertices = allGraphs(7).vertices.filter{
    case (id, vp: ingredient) => vp.usedIn == "main"
    case (id, vp: step) => vp.usedIn == "main"
    case (id, vp: recipe) => id == 1
    case _ => false
    }
    
    val filtered_edges = allGraphs(7).edges.filter{
    case Edge(src, dst, prop: edgeProperty) => prop.usedIn == "main"
    }
    
    val extractedGraph = Graph(filtered_vertices, filtered_edges)
    extractedGraph.triplets.collect.foreach(println)
    
    // Get the side ingredients of recipes_01, for changing name purpose
    
    val side_ingredients_first_recipe: RDD[(Long, ingredient)] = allGraphs(7).vertices.collect {
      case (id, p @ ingredient(name,_,_,itype,usedIn)) if usedIn != "main" => (id, p)
    }
    
    println("####################################")
    println("# Graph: Side process of Recipe_02 #")
    println("####################################")
    
    val filtered_vertices1 = allGraphs(6).vertices.filter{
    case (id, vp: ingredient) => vp.usedIn != "main"
    case (id, vp: step) => vp.usedIn != "main"
    case (id, vp: recipe) => id == 1
    case _ => false
    }
    
    val filtered_edges1 = allGraphs(6).edges.filter{
    case Edge(src, dst, prop: edgeProperty) => prop.usedIn != "main"
    }
    
    val extractedGraph1 = Graph(filtered_vertices1, filtered_edges1)
    extractedGraph1.triplets.collect.foreach(println)
    
    // Get the side ingredients of recipes_02, for changing name purpose
    
    val side_ingredients_second_recipe: RDD[(Long, ingredient)] = allGraphs(6).vertices.collect {
      case (id, p @ ingredient(name,_,_,itype,usedIn)) if usedIn != "main" && itype == "side" => (id, p)
    }
    
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
    
    val extraEdges = sc.makeRDD(Array(Edge(vertexId_receipe_1, dest_id, edgeProperty("step","main")) ))
  
    // Next Step: Combine these two recipes
    
    val totalEdges = final_filtered_edges1.union(extraEdges)  
    
    println("###################################")
    println("######  Graph: New Receipe  #######")
    println("###################################")
    
    val newgraph = Graph( extractedGraph.vertices.union(extractedGraph1.vertices), extractedGraph.edges.union(totalEdges)) 
    
    // Newly generated recipe
    
    val newlyGeneratedRecipe: RDD[(Long, recipe)] = newgraph.vertices.collect {
      case (id, p @ recipe(name,_,_,_,_)) if name != "" => (id, p)
    }
    
    if(side_ingredients_second_recipe.collect().length < 1 || side_ingredients_first_recipe.collect().length < 1)
    {
      println("In first")
      println(side_ingredients_first_recipe.collect().length)
      println(side_ingredients_second_recipe.collect().length)
      for((x,i) <- newlyGeneratedRecipe.collect().view.zipWithIndex) println(x._2.recipeName + ", Cook Time = "+ x._2.cookTime + ", Servings = "+ x._2.servings)
    }
    else
    {
      println("In second")
      println(side_ingredients_first_recipe.collect().length)
      println(side_ingredients_second_recipe.collect().length)
      var recipeNameIndex = 0
      for((x,i) <- newlyGeneratedRecipe.collect().view.zipWithIndex){
      
        for((xs,j) <- side_ingredients_first_recipe.collect().view.zipWithIndex)  {
        
          val wordIndex = x._2.recipeName.toLowerCase indexOf xs._2.ingredientName.toLowerCase
        
          if(wordIndex != -1) {
             println(x._2.recipeName.toLowerCase.replaceAll(xs._2.ingredientName.toLowerCase, side_ingredients_second_recipe.collect()(recipeNameIndex)._2.ingredientName) + ", Cook Time = "+ x._2.cookTime + ", Servings = "+ x._2.servings)
             recipeNameIndex = recipeNameIndex +1
          }
        }
      }
      
      if(recipeNameIndex == 0) for((x,i) <- newlyGeneratedRecipe.collect().view.zipWithIndex) println(x._2.recipeName + ", Cook Time = "+ x._2.cookTime + ", Servings = "+ x._2.servings)
    }
    
    // Ingredients of newly generated recipe
    
    println()
    println("Ingredients:")
    println()
    
    val ingredientVerticesMain: RDD[(Long, ingredient)] = newgraph.vertices.collect {
      case (id, p @ ingredient(_,_,_,_,usedIn)) if usedIn == "main" => (id, p)
    }
    val ingredientVerticesMain1 = ingredientVerticesMain.sortBy(_._1, ascending=true, 1) 
    for((x,i) <- ingredientVerticesMain1.collect().view.zipWithIndex)  println("# " + x._2.ingredientName + ", "+ x._2.quantity+ " "+ x._2.measurementUnit)

    val ingredientVerticesSide: RDD[(Long, ingredient)] = newgraph.vertices.collect {
      case (id, p @ ingredient(_,_,_,_,usedIn)) if usedIn != "main" => (id, p)
    }
    val ingredientVerticesSide1 = ingredientVerticesSide.sortBy(_._1, ascending=true, 1)
    for((x,i) <- ingredientVerticesSide1.collect().view.zipWithIndex) println("# " + x._2.ingredientName + ", "+ x._2.quantity+ " "+ x._2.measurementUnit)
    
    // Steps of newly generated recipe
    
    println()
    println("Instructions:")
    println()
    
    var stepCount = 1
    
    val stepVerticesMain: RDD[(Long, step)] = newgraph.vertices.collect {
      case (id, p @ step(name, _,usedIn)) if usedIn == "main" => (id, p)
    }
    val stepVerticesMain1 = stepVerticesMain.sortBy(_._2.stepNo, ascending=true, 1) 
    for((x,i) <- stepVerticesMain1.collect().view.zipWithIndex){
      println(stepCount + ". " + x._2.description)
      stepCount = stepCount+1
    }

    val stepVerticesSide: RDD[(Long, step)] = newgraph.vertices.collect {
      case (id, p @ step(name,_,usedIn)) if usedIn != "main" => (id, p)
    }
    val stepVerticesSide1 = stepVerticesSide.sortBy(_._2.stepNo, ascending=true, 1)
    for((x,i) <- stepVerticesSide1.collect().view.zipWithIndex){
      println(stepCount + ". " + x._2.description)
      stepCount = stepCount+1
    }
    
    spark.stop
  }
}
  

