import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import com.typesafe.config._
import org.apache.hadoop.fs._

object WordCount {
  def main(args: Array[String]){
        
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
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
    
    val wc = sc.textFile(inputPath).
      flatMap(rec => rec.split(" ")).
      map(rec => (rec,1)).
      reduceByKey((acc, value) => acc + value)
      
    wc.saveAsTextFile(outputPath)
  }
}