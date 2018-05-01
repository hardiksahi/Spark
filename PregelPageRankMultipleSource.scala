package ca.uwaterloo.cs451.GraphXCode
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import java.lang.IllegalArgumentException
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable._
import scala.collection.mutable
import scala.collection.immutable
//import org.apache.spark.broadcast


class PregelPageRankMultipleSourceConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input,src,iterations)
  val input = opt[String](descr = "input path", required = true)
  val src = opt[String](descr = "src", required = true)
  val iterations = opt[String](descr = "iteration", required = true)
  val alpha = opt[String](descr = "alpha", required = true)
  val top = opt[String](descr = "top", required = true)
  verify()
}

object PregelPageRankMultipleSource {
  val log = Logger.getLogger(getClass().getName())
  
  def convertToEdgeList(vertAdjList : Array[String]):List[Tuple2[Long, Long]] = {
    var problemVId = false
    if(vertAdjList.length>1){
      var vertex = vertAdjList(0).toLong
      var adjList:Array[String] = (vertAdjList.drop(1))//.map(_.toInt)
      var retList = new ListBuffer[Tuple2[Long,Long]]()
      for(adjVertex <- adjList){
        var vertexAdj = adjVertex.toLong
        //if(!(vertexAdj>6300 & vertexAdj<0)){
           var tuple: Tuple2[Long, Long] = (vertex, vertexAdj)
           retList+=tuple
        //} 
      }      
      return retList.toList
    }
    else{
      return List()
    }
  }
  
  
  def  sumLogProbs(a: Float, b:Float): Float =  {
		if (a == Float.NegativeInfinity)
			return b;

		if (b == Float.NegativeInfinity)
			return a;

		if (a < b) {
			return (b + StrictMath.log1p(StrictMath.exp(a - b))).toFloat;
		}

		return (a + StrictMath.log1p(StrictMath.exp(b - a))).toFloat;
	}
  
  def getSortDescValues(tuple: Tuple2[Long,Tuple3[Array[Float], Int, Boolean]], srcArray: Array[Long]) : List[Tuple2[Tuple2[Long, Float],Long]] = {
    var vertexId = tuple._1
    var innerTuple = tuple._2
    var prArray = innerTuple._1
    var srcCount = srcArray.length
    
    var index = 0 
    var retList = new ListBuffer[Tuple2[Tuple2[Long, Float], Long]]()
    for(index <- 0 until srcCount){
      var srcVertex = srcArray(index)
      var prForSrc = prArray(index)
      var retTuple:Tuple2[Tuple2[Long, Float],Long] = ((srcVertex, prForSrc),vertexId) // ((srcId, prForSrc),vertexID)
      retList+=retTuple
    }
    return retList.toList   
  }
  
  
  def main(argv: Array[String]) {
    val t1 = System.nanoTime
    val args = new PregelPageRankMultipleSourceConf(argv)
    log.info("Input: " + args.input())
    log.info("Sources:" + args.src())
    log.info("Iteration count:"+ args.iterations())
    log.info("AlphaValue:"  + args.alpha())
    log.info("Top:" + args.top())
    
    val sourceString = args.src()
    val srcIdsArray:Array[Long] = (sourceString.split(",")).map(_.toLong)
    val numberSources = srcIdsArray.length
    
    val iterationCount = (args.iterations()).toInt
    val alphaV = args.alpha().toFloat
    
    val top = (args.top()).toInt
    
    val conf = new SparkConf().setAppName("PregelPageRankMultipleSource")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.getOrCreate
    
    //Step1: Read the adjacency list file
    var inputRDD = sc.textFile(args.input())
    
    //Step2: Convert to edge list to be used to construct graph using GraphX
    var convertedEdgeList = inputRDD.map(vertAdjList =>vertAdjList.split("\\s+")).flatMap(vertAdjList => convertToEdgeList(vertAdjList))
    
    //Step3: Converting edge tuples to graphs
    var graphFromEdgeTuples = Graph.fromEdgeTuples(convertedEdgeList, Float.NegativeInfinity)
    
    //Step4: Initializing properties of vertices, src pagerank = 0, otherwise negativeInfinity
    var pRGraph = graphFromEdgeTuples.mapVertices[Array[Float]]((vId, value) => {
      var indexVId = srcIdsArray.indexOf(vId)
      var arr:Array[Float] = Array.fill(numberSources)(Float.NegativeInfinity)
      if(indexVId == -1){
        arr
      }
      else{
        arr(indexVId) = 0.0f
        arr
      }
    })
    
    var outDegrees:VertexRDD[Int] = pRGraph.outDegrees
    
    //Step5: Modify properties of vertices to include a boolean variable to keep track record of first iteration or not.
    var degreePRGraph = pRGraph.outerJoinVertices(outDegrees){(vertexId, prValueArray, outDegree) => 
        outDegree match{
          case Some(outDegree) => (prValueArray, outDegree, true) // Keeping an extra variable to be able to identify whether we are in 1st iteration or not
          case None => (prValueArray, 0,true) // 0 outdegree for nodes with no outgoing edges
        }
    } // (prArray, outdegree, true/false) => (Array[Float], Int, Boolean)
    
    //Step6: Apply Pregel API for efficient iterations.
    val pprGraph = degreePRGraph.pregel[Array[Float]](Array.fill(numberSources)(Float.NegativeInfinity), iterationCount)((vId:Long, prOutBoolTuple : Tuple3[Array[Float], Int, Boolean], accPr:Array[Float]) => {
     var indexVId = srcIdsArray.indexOf(vId)
      
     if(indexVId != -1){
       // Current Vertex is the source id
       if(prOutBoolTuple._3){
         // Corresponding to 1st iteration
         (prOutBoolTuple._1,prOutBoolTuple._2, false) // False to identify 1st iteration for this node is done.
       }
       else{
         //Not first iteration: Update pr using jump factor.
         var jump = (Math.log(alphaV)).toFloat
         var link = (Math.log(1.0f - alphaV)).toFloat + accPr(indexVId)//sumLogProbs(prArray(indexVId),(Math.log(dmArray(indexVId))).toFloat)
         //var link = (Math.log(1.0f - alphaV)).toFloat + accPr
         var updatedPr = sumLogProbs(jump, link)
         var accPrArray = accPr.map(pr => (StrictMath.log(1.0f - alphaV)).toFloat + pr)
         accPrArray(indexVId) = updatedPr
         (accPrArray, prOutBoolTuple._2, false)
        }
     }
     else{
       //Current vertex not the source
       if(prOutBoolTuple._3){
         (prOutBoolTuple._1,prOutBoolTuple._2, false)
       }
       else{
          var accPrArray = accPr.map(pr => (StrictMath.log(1.0f - alphaV)).toFloat + pr)
          (accPrArray, prOutBoolTuple._2, false) 
       }
     }
    },
    triplet => {
      var srcAttr = triplet.srcAttr
      var srcPrArray = srcAttr._1
      var srcOutDegree = srcAttr._2
      if(srcOutDegree>0){
        srcPrArray = srcPrArray.map(pr => pr-(StrictMath.log(srcOutDegree)).toFloat)
        Iterator((triplet.dstId, srcPrArray))
      }
      else{
        Iterator.empty
      }
      
    },
    (prArray1, prArray2) => prArray1.zip(prArray2).map{case(l,r) => sumLogProbs(l,r)})
    
    //Step7: Sort the output based on page rank values in descending order of (srcVertex, pr)
    var sortedDescVertices = pprGraph.vertices.flatMap(tuple => getSortDescValues(tuple,srcIdsArray)).sortBy(_._1, ascending = false)
    
    //Step8: Filter the sorted to print the output corresponding to source vertices.
    for(srcId <- srcIdsArray){
      sortedDescVertices.filter{case(srcPr, vId) => srcPr._1 == srcId}.top(top).foreach{case(srcPr, vId) => println("Source ID: "+srcId + " PageRank:"+ "%.5f".format(StrictMath.exp(srcPr._2)) + " Vertex ID:" + vId)}
    }
    
    //Step9: Print total time for job execution
    val duration = (System.nanoTime-t1)/1e9d
    println("Time for job execution: " + duration + "s") 
  }
}
