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

class PageRankMultipleSourceConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input,src,iterations)
  val input = opt[String](descr = "input path", required = true)
  val src = opt[String](descr = "src", required = true)
  val iterations = opt[String](descr = "iteration", required = true)
  val alpha = opt[String](descr = "alpha", required = true)
  val top = opt[String](descr = "top", required = true)
  verify()
}

object PageRankMultipleSource {
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
  
  
  def updatePRValue(currentVertex:Long, vertexAttr:Tuple2[Array[Float], Int], srcIdArray:Array[Long], alphaV:Float, count: Int, dmArray:Array[Float]):Tuple2[Array[Float], Int] = {
    
    var prArray = vertexAttr._1
    var outDegree = vertexAttr._2
    var srcCount = srcIdArray.length
    //var prVArray:Array[Float] = Array.fill(srcCount)(0.0f) 
    
    var indexVId = srcIdArray.indexOf(currentVertex)
    
    if(indexVId!= -1){
      //println("Dangling mass received in method in iteration" + count +"is "+ dmArray(indexVId) + "for vertex" + currentVertex)
      var jump = (Math.log(alphaV)).toFloat
      var link = (Math.log(1.0f - alphaV)).toFloat +  sumLogProbs(prArray(indexVId),(Math.log(dmArray(indexVId))).toFloat) // prArray(indexVId)
			var prV = sumLogProbs(jump, link) // to be used for source index..
			prArray = prArray.map(pr => (StrictMath.log(1.0f - alphaV)).toFloat + pr)
			prArray(indexVId) = prV
			//println("Yes for SOURCE and pagerank." + currentVertex + ":" + prV)					
    }
    else{
      prArray = prArray.map(pr => (StrictMath.log(1.0f - alphaV)).toFloat + pr)
    }
    
    //println("At iteration " +count +" PRV previous and new" + vertexAttr._1 + ":" + prV)
    return (prArray, outDegree)
    
  }
  
  def getSortDescValues(tuple: Tuple2[Long,Tuple2[Array[Float], Int]], srcArray: Array[Long]) : List[Tuple2[Tuple2[Long, Float],Long]] = {
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
    val args = new PageRankMultipleSourceConf(argv)
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
    
    val conf = new SparkConf().setAppName("PageRankMultipleSource")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.getOrCreate
    
    //Step1: Read the adjacency list file
    var inputRDD = sc.textFile(args.input())
    
    //Step2: Convert to edge list to be used to construct graph using GraphX
    var convertedEdgeList = inputRDD.map(vertAdjList =>vertAdjList.split("\\s+")).flatMap(vertAdjList => convertToEdgeList(vertAdjList))
    
    //Step3: Converting edge tuples to graphs
    var graphFromEdgeTuples = Graph.fromEdgeTuples(convertedEdgeList, Float.NegativeInfinity)//Array.fill(numberSources)(Float.NegativeInfinity)
    
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
    
    //Step5: Modify properties of vertices to include outdegree of the vertex also
    var degreePRGraph = pRGraph.outerJoinVertices(outDegrees){(vertexId, prValueArray, outDegree) => 
        outDegree match{
          case Some(outDegree) => (prValueArray, outDegree)
          case None => (prValueArray, 0) // 0 outdegree for nodes with no outgoing edges
        }
    }

    
    //Step6:
    //Iterate to perform page rank:
    //1. Calculate page rank mass for all the vertices
    //2. Divide the pagerank mass and send it to the neighbours // sendMsg
    //3. Add the incoming values to each vertex to be able to get a vertex's page rank value in next iteration.//Mergemsg
    //4. Evaluate lost mass[Dangling mass] to be redistributed to source nodes.
    
    var distributePR:VertexRDD[Array[Float]] = degreePRGraph.aggregateMessages[Array[Float]](triplet => {
        var sourceAttr = triplet.srcAttr
        var prValueArray = sourceAttr._1
        var srcOutDegree = sourceAttr._2
        if(srcOutDegree >0){
          prValueArray = prValueArray.map(pr => pr-(StrictMath.log(srcOutDegree)).toFloat)
          triplet.sendToDst(prValueArray)
        } 
      }, 
      (prArray1, prArray2) => prArray1.zip(prArray2).map{case(l,r) => sumLogProbs(l,r)})
      
    degreePRGraph = degreePRGraph.outerJoinVertices(distributePR){(vertexId,oldValVertex,prNewArray) => 
       prNewArray match{
         case Some(prNewArray) => (prNewArray, oldValVertex._2)
         case None => (Array.fill(numberSources)(Float.NegativeInfinity), oldValVertex._2)
    }} 
    
    var totalPrShiftedArray = distributePR.values.reduce((pr1, pr2) => pr1.zip(pr2).map{case(l,r) => sumLogProbs(l,r)})
    
    var evaluatedMassArray = totalPrShiftedArray.map(x => 1.0f - StrictMath.exp(x).toFloat)
    
    var danglingMassArray  =   evaluatedMassArray.map(eMass => {
      if(eMass<0) 0.0f else eMass
    })
    
    degreePRGraph = degreePRGraph.mapVertices((vertexId, attr) => updatePRValue(vertexId, attr,srcIdsArray, alphaV, 1, danglingMassArray))//.cache()
    
    for(count <- 1 until iterationCount){
     distributePR =  degreePRGraph.aggregateMessages[Array[Float]](triplet => {
        var sourceAttr = triplet.srcAttr
        var prValueArray = sourceAttr._1
        var srcOutDegree = sourceAttr._2
        if(srcOutDegree >0){
          prValueArray = prValueArray.map(pr => pr-(StrictMath.log(srcOutDegree)).toFloat)
          triplet.sendToDst(prValueArray)
        } 
      }, 
      (prArray1, prArray2) => prArray1.zip(prArray2).map{case(l,r) => sumLogProbs(l,r)})//.cache()
      
      degreePRGraph = degreePRGraph.outerJoinVertices(distributePR){(vertexId,oldValVertex,prNewArray) => 
       prNewArray match{
         case Some(prNewArray) => (prNewArray, oldValVertex._2)
         case None => (Array.fill(numberSources)(Float.NegativeInfinity), oldValVertex._2)
      }}
     
      totalPrShiftedArray = distributePR.values.reduce((pr1, pr2) => pr1.zip(pr2).map{case(l,r) => sumLogProbs(l,r)})
      
      evaluatedMassArray = totalPrShiftedArray.map(x => 1.0f - StrictMath.exp(x).toFloat)
      
      danglingMassArray  =   evaluatedMassArray.map(eMass => {
        if(eMass<0) 0.0f else eMass
      })
      
      degreePRGraph = degreePRGraph.mapVertices((vertexId, attr) => updatePRValue(vertexId, attr,srcIdsArray, alphaV, count, danglingMassArray))
       
    }
    
    //Step7: Sort the output based on page rank values in descending order of (srcVertex, pr)
    var sortedDescVertices = degreePRGraph.vertices.flatMap(tuple => getSortDescValues(tuple,srcIdsArray)).sortBy(_._1, ascending = false)
    
    //Step8: Filter the sorted to print the output corresponding to source vertices.
    for(srcId <- srcIdsArray){
      sortedDescVertices.filter{case(srcPr, vId) => srcPr._1 == srcId}.top(top).foreach{case(srcPr, vId) => println("Source ID: "+srcId + " PageRank:"+ "%.5f".format(StrictMath.exp(srcPr._2)) + " Vertex ID:" + vId)}
    }
    
    //Step9: Print total time for job execution
    val duration = (System.nanoTime-t1)/1e9d
    println("Time for job execution: " + duration + "s")
  }
}
