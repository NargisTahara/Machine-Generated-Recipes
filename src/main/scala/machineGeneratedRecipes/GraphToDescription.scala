package machineGeneratedRecipes
import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

//import better.files._
//import better.files.File._

import java.util._
import java.io.File
import machineGeneratedRecipes.RecipesCrossOver.AIRecipes
import machineGeneratedRecipes.RecipesCrossOver.recipe
import machineGeneratedRecipes.RecipesCrossOver.ingredient
import machineGeneratedRecipes.RecipesCrossOver.step
import machineGeneratedRecipes.RecipesCrossOver.edgeProperty

import scala.collection.mutable
import scala.io.Source
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.graphframes._
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object GraphToDescription {
  
   val sc = SparkContext.getOrCreate() 
  
  // Convert One entity of Graphx to Plain text
  
   def gToDescription(newgraph: Graph[AIRecipes,edgeProperty], allGraphs: ArrayBuffer[Graph[AIRecipes,edgeProperty]], index1: Int, index2: Int) = {
     val side_ingredients_first_recipe: RDD[(Long, ingredient)] = allGraphs(index1).vertices.collect {
       case (id, p @ ingredient(name,_,_,itype,usedIn)) if usedIn != "main" => (id, p)
       }
     
     val side_ingredients_second_recipe: RDD[(Long, ingredient)] = allGraphs(index2).vertices.collect {
       case (id, p @ ingredient(name,_,_,itype,usedIn)) if usedIn != "main" && itype == "side" => (id, p)
       }
     
     // Newly generated recipe
     val newlyGeneratedRecipes: RDD[recipe] = newgraph.vertices.collect {
       case (id, p @ recipe(name,_,_,_,_,_)) if name != "" => p
       }
     val newlyGeneratedRecipe: Array[recipe] = newlyGeneratedRecipes.take(1)
     
     val newName: String = generateNewName(newlyGeneratedRecipe(0).recipeName, side_ingredients_first_recipe, side_ingredients_second_recipe)
    
     scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll(newName + ", Cook Time = "+ newlyGeneratedRecipe(0).cookTime + ", Servings = "+ newlyGeneratedRecipe(0).servings)
    
    // Ingredients of newly generated recipe
    
    scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll("\n\nIngredients:\n\n")
    
    val ingredientVerticesMain: RDD[(Long, ingredient)] = newgraph.vertices.collect {
      case (id, p @ ingredient(_,_,_,_,usedIn)) if usedIn == "main" => (id, p)
    }
    val ingredientVerticesMain1 = ingredientVerticesMain.sortBy(_._1, ascending=true, 1) 
    for((x,i) <- ingredientVerticesMain1.collect().view.zipWithIndex)  scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll("# " + x._2.ingredientName + ", "+ x._2.quantity+ " "+ x._2.measurementUnit+ "\n")

    val ingredientVerticesSide: RDD[(Long, ingredient)] = newgraph.vertices.collect {
      case (id, p @ ingredient(_,_,_,_,usedIn)) if usedIn != "main" => (id, p)
    }
    val ingredientVerticesSide1 = ingredientVerticesSide.sortBy(_._1, ascending=true, 1)
    for((x,i) <- ingredientVerticesSide1.collect().view.zipWithIndex) scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll("# " + x._2.ingredientName + ", "+ x._2.quantity+ " "+ x._2.measurementUnit+ "\n")
    
    // Steps of newly generated recipe
    
    scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll("\n\nInstructions:\n\n")
    
    var stepCount = 1
    
    val stepVerticesMain: RDD[(Long, step)] = newgraph.vertices.collect {
      case (id, p @ step(name, _,usedIn)) if usedIn == "main" => (id, p)
    }
    val stepVerticesMain1 = stepVerticesMain.sortBy(_._2.stepNo, ascending=true, 1) 
    for((x,i) <- stepVerticesMain1.collect().view.zipWithIndex){
      scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll(stepCount + ". " + x._2.description + "\n")
      stepCount = stepCount+1
    }

    val stepVerticesSide: RDD[(Long, step)] = newgraph.vertices.collect {
      case (id, p @ step(name,_,usedIn)) if usedIn != "main" => (id, p)
    }
    val stepVerticesSide1 = stepVerticesSide.sortBy(_._2.stepNo, ascending=true, 1)
    for((x,i) <- stepVerticesSide1.collect().view.zipWithIndex){
      scala.tools.nsc.io.File("data/recipesOutput.txt").appendAll(stepCount + ". " + x._2.description + "\n")
      stepCount = stepCount+1
    }
  }
   
   // Read all recipes from given file
   
     def readAllrecipesFromFileToGraph(filename: String) : ArrayBuffer[Graph[AIRecipes,edgeProperty]] = {
       var current = "recipe"
       var vertexArray = Array.empty[(Long, AIRecipes)]
       var edgeArray = Array.empty[Edge[edgeProperty]]
       var allGraphs = ArrayBuffer[Graph[AIRecipes,edgeProperty]]()
    
      for (line <- Source.fromFile(filename).getLines) {  
        val arr = line.split("-")
        var testObject: AIRecipes = null
      
        // Set which class to populate 
        val sc = SparkContext.getOrCreate()  
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
            testObject = recipe(arr(1),arr(2),arr(3).toInt,arr(4),arr(5).toInt,arr(6))
            vertexArray = vertexArray ++ Array((arr(0).toLong, testObject))
          }
        }
      }
      return allGraphs
    }
     
      // Read all recipes from given file
   
     def swapTworecipes(allGraphs: ArrayBuffer[Graph[AIRecipes,edgeProperty]], index1: Int, index2: Int) : Graph[AIRecipes,edgeProperty] = {
       
       //println("####################################")
       //println("# Graph: Main process of Recipe_01 #")
       //println("####################################")
       
       val filtered_vertices = allGraphs(index1).vertices.filter{
       case (id, vp: ingredient) => vp.usedIn == "main"
       case (id, vp: step) => vp.usedIn == "main"
       case (id, vp: recipe) => id == 1
       case _ => false
       }
    
       val filtered_edges = allGraphs(index1).edges.filter{
       case Edge(src, dst, prop: edgeProperty) => prop.usedIn == "main"
       }
    
       val extractedGraph = Graph(filtered_vertices, filtered_edges)
       //extractedGraph.vertices.collect.foreach(println)
       //extractedGraph.edges.collect.foreach(println)
    
       // Get the side ingredients of recipes_01, for changing name purpose
    
       val side_ingredients_first_recipe: RDD[(Long, ingredient)] = allGraphs(index1).vertices.collect {
         case (id, p @ ingredient(name,_,_,itype,usedIn)) if usedIn != "main" => (id, p)
       }
    
       //println("####################################")
       //println("# Graph: Side process of Recipe_02 #")
       //println("####################################")
    
       val filtered_vertices1 = allGraphs(index2).vertices.filter{
       case (id, vp: ingredient) => vp.usedIn != "main"
       case (id, vp: step) => vp.usedIn != "main"
       case (id, vp: recipe) => id == 1
       case _ => false
      }
    
      val filtered_edges1 = allGraphs(index2).edges.filter{
      case Edge(src, dst, prop: edgeProperty) => prop.usedIn != "main"
      }
    
      val extractedGraph1 = Graph(filtered_vertices1, filtered_edges1)
      //extractedGraph1.triplets.collect.foreach(println)
    
      // Get the side ingredients of recipes_02, for changing name purpose
    
      val side_ingredients_second_recipe: RDD[(Long, ingredient)] = allGraphs(index2).vertices.collect {
        case (id, p @ ingredient(name,_,_,itype,usedIn)) if usedIn != "main" && itype == "side" => (id, p)
      }
    
      // Finding vertex with null property 
    
      val vertexWithNullProperty = extractedGraph1.vertices.filter{
      case (id, vp) => vp == null
      }
      val vertexWithNullProperty_1 = vertexWithNullProperty.take(1)
      val vertexWithNullProperty_id = vertexWithNullProperty_1(0)._1
    
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
       val newgraph = Graph(extractedGraph.vertices.union(extractedGraph1.vertices), extractedGraph.edges.union(totalEdges))
       return newgraph
     }
     
     // Function to generate a new recipe name from parent recipes names and from parent recipes ingredients 
     def generateNewName(currentName: String, side_ingredients_first_recipe: RDD[(Long, ingredient)], side_ingredients_second_recipe: RDD[(Long, ingredient)]) : String = {
       
       var newName: String = currentName      
       var recipeNameIndex = 0
       
       for((xs,j) <- side_ingredients_first_recipe.collect().view.zipWithIndex){ 
         
        println("RecipeIndex: " + recipeNameIndex) 
         
        val wordIndex = currentName.toLowerCase indexOf xs._2.ingredientName.toLowerCase
        val s_ingredientName = xs._2.ingredientName.substring(0, xs._2.ingredientName.length()-1)
        val s_wordIndex = currentName.toLowerCase indexOf s_ingredientName
        
        println(s_ingredientName)
        println(s_wordIndex)
        
        if( (wordIndex != -1 || s_wordIndex != -1)  && recipeNameIndex < side_ingredients_second_recipe.collect().length) {
           println("In recipe name change process")
           newName = newName.toLowerCase.replaceAll(xs._2.ingredientName.toLowerCase, side_ingredients_second_recipe.collect()(recipeNameIndex)._2.ingredientName)
           newName = newName.toLowerCase.replaceAll(xs._2.ingredientName.toLowerCase, s_ingredientName)
           recipeNameIndex = recipeNameIndex +1
        }
        else if( (wordIndex != -1 || s_wordIndex != -1) && recipeNameIndex >= side_ingredients_second_recipe.collect().length){
          println("Removeal Process" + xs._2.ingredientName.toLowerCase)
          
          
          newName = newName.toLowerCase.replaceAll(xs._2.ingredientName + " ,", "")
          newName = newName.toLowerCase.replaceAll(xs._2.ingredientName + ",", "")
          newName = newName.toLowerCase.replaceAll(xs._2.ingredientName, "")
          
          println("removal new name 1: " + newName)
          
          newName = newName.toLowerCase.replaceAll(s_ingredientName.toLowerCase + " ,", "")
          newName = newName.toLowerCase.replaceAll(s_ingredientName.toLowerCase + ",", "")
          newName = newName.toLowerCase.replaceAll(s_ingredientName.toLowerCase, "")
          
          println("removal new name 2: " + newName)
            
          val withIndex = newName.toLowerCase indexOf "with"
          if( withIndex == (newName.size - 5)) newName = newName.substring(0, withIndex - 1)
            
          val andIndex = newName.toLowerCase indexOf "and"
          if( andIndex == (newName.size - 4)) newName = newName.substring(0, andIndex - 1)
        }
       }
      
        if(recipeNameIndex == 0 && side_ingredients_second_recipe.collect().length > 0){
           if(side_ingredients_second_recipe.collect().length > 1) newName = currentName + " with " + side_ingredients_second_recipe.collect()(0)._2.ingredientName + " and " + side_ingredients_second_recipe.collect()(1)._2.ingredientName
           else newName = currentName + " with " + side_ingredients_second_recipe.collect()(0)._2.ingredientName     
        }
       
       return newName
     }
     
      def isCompatible(allGraphs: ArrayBuffer[Graph[AIRecipes,edgeProperty]], index1: Int, index2: Int) : Boolean = {
        
        val recipes_1: RDD[recipe] = allGraphs(index1).vertices.collect {
         case (id, p @ recipe(name,_,_,_,_,_)) if name != "" => p
        }
        val recipe_01: Array[recipe] = recipes_1.take(1)
        
        
        val recipes_2: RDD[recipe] = allGraphs(index2).vertices.collect {
         case (id, p @ recipe(name,_,_,_,_,_)) if name != "" => p
        }
        val recipe_02: Array[recipe] = recipes_2.take(1)
        
        if(recipe_01(0).cookedType_mainIngredient == "notCooked" && recipe_02(0).cookedType_mainIngredient == "cooked")
          return false
        
        return true
      }
      
       def replaceMainIngredientNameinInstruction(step: String, mainIngredientRecipe2: String) : String = {
        
        val wordIndex = step.toLowerCase indexOf mainIngredientRecipe2.toLowerCase()
        var newStep = ""
        
        if(wordIndex != 1)
          newStep = step.toLowerCase.replaceAll(mainIngredientRecipe2.toLowerCase(), "potatoe")
        else{
          // To solve plular issues  
          val mainIng: String = mainIngredientRecipe2.substring(0,mainIngredientRecipe2.length()-1)
          newStep = step.toLowerCase.replaceAll(mainIng.toLowerCase(), "potatoe")
        }
         
        return newStep
      }
}
  

