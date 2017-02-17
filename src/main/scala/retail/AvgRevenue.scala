package retail

import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import com.typesafe.config._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AvgRevenue {
  def main(args: Array[String]){
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Average Revenue").
      setMaster(appConf.getConfig(args(2)).getString("deploymentMaster"))
      
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outputPathExists = fs.exists(new Path(outputPath))
    
    if(!inputPathExists){
      println("Input path does not exist")
      return
    }
    
    if(outputPathExists){
      fs.delete(new Path(outputPath),true)
    }
    
    val ordersRDD = sc.textFile(inputPath + "/orders")
    val orderItemsRDD = sc.textFile(inputPath + "/order_items")
    
    val ordersCompleted = ordersRDD.
      filter(rec => (rec.split(",")(3) == "COMPLETE"))
      
    val orders = ordersCompleted.
      map(rec => (rec.split(",")(0).toInt,rec.split(",")(1)))
    val orderItemsMap = orderItemsRDD .
      map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
      
    val orderItems = orderItemsMap.
      reduceByKey((acc, value) => acc + value)
    val ordersJoin = orders.join(order_items)
    
    val ordersJoinMap = ordersJoin.map(rec => (rec._2._1, rec._2._2))
    
    
  }
}