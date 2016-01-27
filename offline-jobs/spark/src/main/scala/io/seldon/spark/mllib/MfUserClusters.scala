package io.seldon.spark.mllib

/**
 * @author clive
 */
import io.seldon.spark.rdd.FileUtils
import io.seldon.spark.rdd.DataSourceMode
import io.seldon.spark.zookeeper.ZkCuratorHandler
import java.io.File
import java.text.SimpleDateFormat
import org.apache.curator.utils.EnsurePath
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable
import scala.util.Random._

case class UCActionWeightings (actionMapping: List[UCActionWeighting])

case class UCActionWeighting (actionType:Int = 1,
                             valuePerAction:Double = 1.0,
                             maxSum:Double = 1.0
                             )
case class UCMfConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,    
    zkHosts : String = "",
    activate : Boolean = false,
    
    rank : Int = 30,
    lambda : Double = 0.01,
    alpha : Double = 1,
    iterations : Int = 20,
    kmeansIterations : Int = 200,
    numRecommendations : Int = 200,
    actionWeightings: Option[List[UCActionWeighting]] = None,
    clusters : Int = 2
 )
 

class MfUserClusters(private val sc : SparkContext,config : UCMfConfig) {

 
  
  def run() 
  {
    val client = config.client
    val date:Int = config.startDay
    val daysOfActions = config.days
    val rank = config.rank
    val lambda = config.lambda
    val alpha = config.alpha
    val iterations = config.iterations
    val zkServer = config.zkHosts
    val inputFilesLocation = config.inputPath + "/" + config.client + "/actions/"
    val inputDataSourceMode = DataSourceMode.fromString(inputFilesLocation)
    if (inputDataSourceMode == DataSourceMode.NONE) {
      println("input file location must start with local:// or s3n://")
      sys.exit(1)
    }
    val outputFilesLocation = config.outputPath + "/" + config.client +"/mfclusters/" + "/" + config.startDay
    val outputDataSourceMode = DataSourceMode.fromString(outputFilesLocation)
    if (outputDataSourceMode == DataSourceMode.NONE) {
      println("output file location must start with local:// or s3n://")
      sys.exit(1)
    }
    val actionWeightings = config.actionWeightings.getOrElse(List(UCActionWeighting()))
    // any action not mentioned in the weightings map has a default score of 0.0
    val weightingsMap = actionWeightings.map(
      aw => (aw.actionType, (aw.valuePerAction,aw.maxSum))
    ).toMap.withDefaultValue(.0,.0)

    println("Using weightings map"+weightingsMap)
    val startTime = System.currentTimeMillis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // set up environment
    val timeStart = System.currentTimeMillis()
    val glob= inputFilesLocation + ((date - daysOfActions + 1) to date).mkString("{", ",", "}")
    println("Looking at "+glob)
    val actions:RDD[((Int, Int), Int)] = sc.textFile(glob)
      .map { line =>
        val json = parse(line)
        import org.json4s._
        implicit val formats = DefaultFormats
        val user = (json \ "userid").extract[Int]
        val item = (json \ "itemid").extract[Int]
        val actionType = (json \"type").extract[Int]
        ((item,user),actionType)
      }.repartition(2).cache()

    // group actions by user-item key
    val actionsByType:RDD[((Int,Int),List[Int])] = actions.combineByKey((x:Int)=>List[Int](x),
                                                                        (list:List[Int],y:Int)=>y :: list,
                                                                        (l1:List[Int],l2:List[Int])=> l1 ::: l2)
    // count the grouped actions by action type
    val actionsByTypeCount:RDD[((Int,Int),Map[Int,Int])] = actionsByType.map{
      case (key,value:List[Int]) => (key,value.groupBy(identity).mapValues(_.size).map(identity))
    }

    // apply weigtings map
    val actionsByScore:RDD[((Int,Int),Double)] = actionsByTypeCount.mapValues{
      case x:Map[Int,Int]=> x.map {
        case y: (Int, Int) => Math.min(weightingsMap(y._1)._1 * y._2, weightingsMap(y._1)._2)
      }.reduce(_+_)
    }

    actionsByScore.take(10).foreach(println)
    val itemsByCount: RDD[(Int, Int)] = actionsByScore.map(x => (x._1._1, 1)).reduceByKey(_ + _)
    val itemsCount = itemsByCount.count()
    println("total actions " + actions.count())
    println("total items " + itemsByCount.count())

    val usersByCount = actionsByScore.map(x=>(x._1._2,1)).reduceByKey(_+_)
    val usersCount = usersByCount.count()
    println("total users " + usersCount)

    val ratings = actionsByScore.map{
      case ((product, user), rating) => Rating(user,product,rating)
    }.repartition(2).cache()

    val timeFirst = System.currentTimeMillis()
    println("munging data took "+(timeFirst-timeStart)+"ms")
    val model: MatrixFactorizationModel = ALS.trainImplicit(ratings = ratings, rank = rank, iterations = iterations, lambda = lambda, alpha = alpha, blocks = -1)
    println("training model took "+(System.currentTimeMillis() - timeFirst)+"ms")
    
   
    println("Running SVD")
    val productRows= model.productFeatures.map(s => Vectors.dense(s._2))
    val productRowMatrix = new RowMatrix(productRows)
    val userRows = model.userFeatures.map(s => Vectors.dense(s._2))
    val userRowMatrix = new RowMatrix(userRows)
    val productSVD = productRowMatrix.computeSVD(10, computeU = true)

    println("Creating euclidean space user features")
    val userEuclideanFeatures= userRowMatrix.multiply(productSVD.V).multiply(Matrices.diag(productSVD.s))

   
    val numClusters = config.clusters
    val numIterations = config.kmeansIterations
    println("Running kmeans with "+numClusters+" clusters for "+numIterations+" iterations")
    val userClusterModel = KMeans.train(userEuclideanFeatures.rows.cache(), numClusters, numIterations)
    

    println("Creating architype users from cluster centres and putting into inner product space")
    // create user archtype vectors from cluster centres
    val centreRdd = new RowMatrix(sc.parallelize(userClusterModel.clusterCenters))
    val invS = Matrices.diag(Vectors.dense(productSVD.s.toArray.map(x => math.pow(x,-1))))
    val userArchtypes = centreRdd.multiply(invS).multiply(productSVD.V.transpose)
    val userArchFactors = userArchtypes.rows.zipWithIndex().map(v => (v._2.toInt,v._1.toArray)).cache()
    val modelNew = new MatrixFactorizationModel(model.rank,userArchFactors,model.productFeatures)
    
    println("Creating predictions for archtype users")
    val numRecommendations = 200
    val predictions = modelNew.recommendProductsForUsers(numRecommendations).flatMapValues{v => v}.map{case (user,rating) => (user,rating.product,rating.rating)}
    val predictionsStr = predictions.map{case (user,product,rating) => 
      var line = new StringBuilder()
      line ++= user.toString()
      line ++= ","
      line ++= product.toString()
      line ++= ","
      line ++= rating.toString()
      line.toString()
      }
    FileUtils.outputModelToFile(predictionsStr, outputFilesLocation, DataSourceMode.fromString(outputFilesLocation), "recommendations.csv")
    
    //create cluster assignments for users
    println("Creating user cluster id map")
    val bUserClusterModel = sc.broadcast(userClusterModel)
    val dd = userEuclideanFeatures.rows.zip(model.userFeatures)// dp similar but get user ids and zip with eucidean features and then do below on that to get clusters
    val userClusterMap = dd.map{case (euclideanFeats,(user,_)) => (user,bUserClusterModel.value.predict(euclideanFeats))}
    val userClusterMapStr = userClusterMap.map{case (user,cluster) => 
      var line = new StringBuilder()
      line ++= user.toString()
      line ++= ","
      line ++= cluster.toString()
      line.toString()
      }
   FileUtils.outputModelToFile(userClusterMapStr, outputFilesLocation, DataSourceMode.fromString(outputFilesLocation), "userclusters.csv")

    
    
    if (config.activate)
    {
      val curator = new ZkCuratorHandler(zkServer)
      if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
      {
        val zkPath = "/all_clients/"+client+"/mf"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,(outputFilesLocation+date).getBytes())
      }
    }
    
    println(List(rank,lambda,iterations,alpha,0,System.currentTimeMillis() - timeFirst).mkString(","))

    //model.userFeatures.unpersist()
    //model.productFeatures.unpersist()
    // val rdds = sc.getRDDStorageInfo
    //if(sc.getPersistentRDDs !=null) 
   // {
    //    for (rdd <- sc.getPersistentRDDs.values) {
     //     if (rdd.name != null && (rdd.name.startsWith("product") || rdd.name.startsWith("user"))) {
     //       rdd.unpersist(true);
     //     }
     //   }
   /// }

    sc.stop()
    println("Time taken " + (System.currentTimeMillis() - startTime))
  }

  
  

  

  

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}

/**
 * @author firemanphil
 *         Date: 14/10/2014
 *         Time: 15:16
 *
 */
object MfUserClusters {

 
  def updateConf(config : UCMfConfig) =
  {
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/matrix-factorization"
       if (curator.getCurator.checkExists().forPath(path) != null)
       {
         val bytes = curator.getCurator.getData().forPath(path)
         val j = new String(bytes,"UTF-8")
         println("Configuration from zookeeper -> ",j)
         import org.json4s._
         import org.json4s.jackson.JsonMethods._
         implicit val formats = DefaultFormats
         val json = parse(j)
         import org.json4s.JsonDSL._
         import org.json4s.jackson.Serialization.write
         type DslConversion = UCMfConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[UCMfConfig] // extract case class from merged json
         c
       }
       else 
       {
           println("Warning: using default configuration - path["+path+"] not found!");
           c
       }
     }
     else 
     {
       println("Warning: using default configuration - no zkHost!");
       c
     }
  }
  

  def main(args: Array[String]) 
  {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    var c = new UCMfConfig()
    val parser = new scopt.OptionParser[Unit]("MatrixFactorization") {
    head("ClusterUsersByDimension", "1.x")
        opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
        opt[Unit]("activate") foreach { x => c = c.copy(activate = true) } text("activate the model in the Seldon Server")
        
        opt[Int]('u', "rank") foreach { x =>c = c.copy(rank = x) } text("the number of latent factors in the model")
        opt[Double]('m', "lambda") foreach { x =>c = c.copy(lambda = x) } text("the regularization parameter in ALS to stop over-fitting")
        opt[Double]('m', "alpha") foreach { x =>c = c.copy(alpha = x) } text("governs the baseline confidence in preference observations")        
        opt[Int]('u', "iterations") foreach { x =>c = c.copy(iterations = x) } text("the number of iterations to run the modelling")
        opt[Int]("clusters") foreach { x =>c = c.copy(clusters = x) } text("number of user clusters to create")
        opt[Int]("kmeansIterations") foreach { x =>c = c.copy(kmeansIterations = x) } text("number of kmeans iterations to run")
        opt[Int]("numRecommendations") foreach { x =>c = c.copy(numRecommendations = x) } text("number of recommendations per user to create")
    }
    
    
    if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line
      
      val conf = new SparkConf().setAppName("MatrixFactorization")

      if (c.local)
        conf.setMaster("local")
        .set("spark.executor.memory", "8g")

      val sc = new SparkContext(conf)
      try
      {
        sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if (c.awsKey.nonEmpty && c.awsSecret.nonEmpty)
        {
         sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", c.awsKey)
         sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", c.awsSecret)
        }
        println(c)
        val mf = new MfUserClusters(sc,c)
        mf.run()
      }
      finally
      {
        println("Shutting down job")
        sc.stop()
      }
   } 
   else 
   {
      
   }


  }

  
}
