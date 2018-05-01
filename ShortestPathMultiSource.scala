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

class ShortestPathMultiSourceConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input,src,iterations)
  val input = opt[String](descr = "input path", required = true)
  val src = opt[String](descr = "src", required = true)
  val iterations = opt[String](descr = "iteration", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object ShortestPathMultiSource {
  val log = Logger.getLogger(getClass().getName())

  def convertToEdgeList(vertAdjList : Array[String]):List[Tuple2[Long, Long]] = {
    var problemVId = false
    if(vertAdjList.length>1){
      var vertex = vertAdjList(0).toLong
      var adjList:Array[String] = (vertAdjList.drop(1))//.map(_.toInt)
      var retList = new ListBuffer[Tuple2[Long,Long]]()
      for(adjVertex <- adjList){
        var vertexAdj = adjVertex.toLong
        var tuple: Tuple2[Long, Long] = (vertex, vertexAdj)
        retList+=tuple
        
      }
      return retList.toList
    }
    else{
      return List()
    }
  }

  def main(argv: Array[String]) {
    val t1 = System.nanoTime
    val args = new ShortestPathMultiSourceConf(argv)
    log.info("Input: " + args.input())
    log.info("Source:" + args.src())
    log.info("Iteration count:"+ args.iterations())
	  log.info("Output: " + args.output())
	
    val sources = args.src()
    val srcIdsArray:Array[Long] = (sources.split(",")).map(_.toLong)
    val numberSources = srcIdsArray.length
    val iterationCount = (args.iterations()).toInt

    val conf = new SparkConf().setAppName("ShortestPathMultiSource")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.getOrCreate
	
	  val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
	   
    //Step1: Read the adjacency list file
    var inputRDD = sc.textFile(args.input())
    
    //Step2: Convert to edge list to be used to construct graph using GraphX
    var convertedEdgeList = inputRDD.map(vertAdjList =>vertAdjList.split("\\s+")).flatMap(vertAdjList => convertToEdgeList(vertAdjList))
    
    //Step3: Converting edge tuples to graphs.
    var graphFromEdgeTuples = Graph.fromEdgeTuples(convertedEdgeList, Float.PositiveInfinity)
    
    //Step4: Initializing properties of vertices, src dist = 0, otherwise positiveInfinity
    var distanceGraph = graphFromEdgeTuples.mapVertices[Array[Float]]((vId, distance) =>{
            var indexVId = srcIdsArray.indexOf(vId)
            var distArray:Array[Float] = Array.fill(numberSources)(Float.PositiveInfinity)
            if(indexVId == -1)
                   distArray
            else {
                   distArray(indexVId) = 0.0f
                   distArray
            }
    })
    
    //Step5: Iterating.
    for(i <- 0 until iterationCount){
      var distributePR:VertexRDD[Array[Float]] =  distanceGraph.aggregateMessages[Array[Float]](triplet => {
			var srcDistValueArray = triplet.srcAttr
            var flag = false
            var srcSendToDestArray = srcDistValueArray.map{dist => {
                if(dist != Float.PositiveInfinity){
                        flag = true
                        dist+triplet.attr//1.0f
                }
                else{
                        dist
                }
                }
            }

                if(flag){
                        triplet.sendToDst(srcSendToDestArray)
                        triplet.sendToSrc(srcDistValueArray)
                }
			  },
                (dist1Array, dist2Array) => dist1Array.zip(dist2Array).map{case(l,r) => math.min(l,r)})

			//distanceGraph = distanceGraph.joinVertices(distributePR)((vertexId,oldDistanceArray,updatedDistanceArray) => (updatedDistanceArray))
			distanceGraph = distanceGraph.outerJoinVertices(distributePR){(vertexId,oldDistanceArray,updatedDistanceArray) =>
                updatedDistanceArray match{
					case Some(updatedDistanceArray) => updatedDistanceArray
					case None => oldDistanceArray
				}
			}
    }
    
    //Step6: Map over output to convert to approriate output format, save to file.
    distanceGraph.vertices.map{case(vId, distArray) => (vId, distArray.mkString(" | "))}.saveAsTextFile(args.output())//foreach{case(vId, dist) => println("Distance from src" + vId + "is "+dist.mkString(","))}
    
    //Step7: Print total time for job execution
    val duration = (System.nanoTime-t1)/1e9d
    println("Time for job execution: " + duration + "s")  
  }

 }

