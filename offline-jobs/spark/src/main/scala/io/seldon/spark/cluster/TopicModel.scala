package io.seldon.spark.cluster


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import util.Random.nextInt
import java.io.File
import java.sql.{DriverManager,ResultSet}
import collection.mutable.{ HashMap, MultiMap, Set }
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.Period
import io.seldon.spark.SparkUtils
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.text.BreakIterator
import scala.collection.mutable
import io.seldon.spark.rdd.FileUtils
import io.seldon.spark.rdd.DataSourceMode
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.clustering.DistributedLDAModel


case class TopicConfig(
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

    jdbc : String = "",
    tagAttr : String = "",
    minActionsPerUser : Int = 10,
    numTopics : Int = 50,
    maxIterations : Int = 20
    )

class TopicModel(private val sc : SparkContext,config : TopicConfig) {
 
  def activate(location : String) 
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    import org.apache.curator.utils.EnsurePath
    val curator = new ZkCuratorHandler(config.zkHosts)
    if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
    {
        val zkPath = "/all_clients/"+config.client+"/topics"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,location.getBytes())
    }
    else
      println("Failed to get zookeeper! Can't activate model")
  }
  
  def parseJsonActions(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
    
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      (user,item)
      }
    
    rdd
  }
  
   def getItemTagsFromDb(jdbc : String,attr : String) = 
  {
//    val sql = "select i.item_id,i.client_item_id,unix_timestamp(first_op),tags.value as tags from items i join item_map_"+table+" tags on (i.item_id=tags.item_id and tags.attr_id="+tagAttrId.toString()+") where i.item_id>? and i.item_id<?"
    val sql = "select * from (SELECT i.item_id,i.client_item_id,unix_timestamp(first_op),CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END" +  
      " tags FROM  items i INNER JOIN item_attr a ON a.name in ('"+attr+"') and i.type=a.item_type LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id " +
      " where i.item_id>? and i.item_id<? order by imv.pos) t where not t.tags is null"

    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    sql,
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getString("tags").toLowerCase().trim())
    )
    rdd
  }
  
  def getFilteredActions(minActions : Int,actions : org.apache.spark.rdd.RDD[(Int,Int)]) = {

    actions.groupBy(_._1).filter(_._2.size >= minActions).flatMap(_._2).map(v => (v._2,v._1)) // filter users with no enough actions and transpose to item first
  }
 
  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
    println("Loading tags from "+config.jdbc)
    val rddActions = getFilteredActions(config.minActionsPerUser, parseJsonActions(actionsGlob))

    // get item tags from db

    val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttr)
    
    // sort by item_id and join actions and tags
    val rddCombined = rddActions.join(rddItems)
    
    val userTags : RDD[(Long, IndexedSeq[String])] = rddCombined.map(_._2).flatMapValues(_.split(",")).mapValues(_.trim()).filter(_._2.size > 0)
          .reduceByKey((tag1,tag2) => (tag1 +" " + tag2))
          .map { case (id,tagStr) =>  
            val words = new mutable.ArrayBuffer[String]()
            val tags = tagStr.split("\\s+")
            for(tag <- tags)
            {
              words += tag
            }
            (id.toLong,words)
            }

    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens) = preprocess(sc, userTags, -1)
    corpus.cache()
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.size
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9
    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()
    // Run LDA.
    val lda = new LDA()
    lda.setK(config.numTopics)
    .setMaxIterations(config.maxIterations)

    val startTime = System.nanoTime()
    val ldaModel =  lda.run(corpus).asInstanceOf[DistributedLDAModel]
    val elapsed = (System.nanoTime() - startTime) / 1e9
    println(s"Finished training LDA model. Summary:")
    println(s"\t Training time: $elapsed sec")
    val avgLogLikelihood = ldaModel.logLikelihood / actualCorpusSize.toDouble
    println(s"\t Training data average log likelihood: $avgLogLikelihood")
    println()
    
    val outPath = config.outputPath + "/" + config.client + "/topics/"+config.startDay
    
    val topicDistr = ldaModel.topicDistributions.collectAsMap()
    val userTopics = new ArrayBuffer[String]
    for( (id,vec) <- topicDistr)
    {
      var line = new StringBuilder()
      line ++= id.toString()
      vec.toArray.zipWithIndex.map{ case (v,i) => 
        line ++= ","
        line ++= (i+1).toString()
        line ++= ":"
        line ++= format("%1.5f", v)
        }
      userTopics.append(line.toString())
    }
    
    FileUtils.outputModelToFile(userTopics.toArray, outPath, DataSourceMode.fromString(outPath), "user.csv")

    val topicIndices = ldaModel.describeTopics(50)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    val termWeights = topics.zipWithIndex
      .map { case (topic, i) =>
          topic.map { case (term, weight) =>
             (term,(i+1,weight))
//             ""+i+","+term+","+weight
        }
      }.flatten
      
      /*
      val termWeightsRdd = sc.parallelize(termWeights)
      
      val sparseTerms = termWeightsRdd.reduceByKey{ case ((i1,w1),(i2,w2)) =>
        if (w1>w2)
          (i1,w1)
        else
          (i2,w2)
          }
      */
      val termsAsStr = termWeights.map{case (term,(topic,weight)) => ""+topic+","+term+","+format("%1.7f", weight)}

    
    
    FileUtils.outputModelToFile(termsAsStr, outPath, DataSourceMode.fromString(outPath), "topics.csv")
    
    //userTagCounts.saveAsTextFile(outPath)
    
    sc.stop()       
  }
  
  
  private def preprocess(sc: SparkContext,tokenized: RDD[(Long, IndexedSeq[String])],vocabSize: Int): (RDD[(Long, Vector)], Array[String], Long) = {

  tokenized.cache()
  // Counts words: RDD[(word, wordCount)]
  val wordCounts: RDD[(String, Long)] = tokenized.flatMap { case (_, tokens) => tokens.map(_ -> 1L) }.reduceByKey(_ + _)
  wordCounts.cache()
  val fullVocabSize = wordCounts.count()
  // Select vocab
  // (vocab: Map[word -> id], total tokens after selecting vocab)
  val (vocab: Map[String, Int], selectedTokenCount: Long) = {
  val tmpSortedWC: Array[(String, Long)] = 
      if (vocabSize == -1 || fullVocabSize <= vocabSize) {
    // Use all terms
    wordCounts.collect().sortBy(-_._2)
    } else {
    // Sort terms to select vocab
    wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
    }
  (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
  }

  val documents = tokenized.map { case (id, tokens) =>
  // Filter tokens by vocabulary, and create word count vector representation of document.
  val wc = new mutable.HashMap[Int, Int]()
  tokens.foreach { term =>
  if (vocab.contains(term)) {
  val termIndex = vocab(term)
  wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
  }
  }
  val indices = wc.keys.toArray.sorted
  val values = indices.map(i => wc(i).toDouble)
  val sb = Vectors.sparse(vocab.size, indices, values)
  (id, sb)
  }
  val vocabArray = new Array[String](vocab.size)
  vocab.foreach { case (term, i) => vocabArray(i) = term }
    (documents, vocabArray, selectedTokenCount)
  }
}


object TopicModel
{
   def updateConf(config : TopicConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/topic-lda"
       if (curator.getCurator.checkExists().forPath(path) != null)
       {
         val bytes = curator.getCurator.getData().forPath(path)
         val j = new String(bytes,"UTF-8")
         println("Confguration from zookeeper -> "+j)
         import org.json4s._
         import org.json4s.jackson.JsonMethods._
         implicit val formats = DefaultFormats
         val json = parse(j)
         import org.json4s.JsonDSL._
         import org.json4s.jackson.Serialization.write
         type DslConversion = TopicConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[TopicConfig] // extract case class from merged json
         c
       }
       else 
       {
           println("Warning: using default configuaration - path["+path+"] not found!");
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

    var c = new TopicConfig()
    val parser = new scopt.OptionParser[Unit]("TopicModel") {
    head("TopicModel", "1.0")
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

        opt[String]('j', "jdbc") valueName("<JDBC URL>") foreach { x => c = c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
        opt[String]("tagAttr") valueName("tag attribute") foreach { x => c = c.copy(tagAttr = x) } text("tag attribute")        
        opt[Int]('m', "minActionsPerUser") foreach { x => c = c.copy(minActionsPerUser = x) } text("min number of actions per user")
        opt[Int]('t', "numTopics") foreach { x => c = c.copy(numTopics = x) } text("number of topics")
        opt[Int]("maxIterations") foreach { x => c = c.copy(maxIterations = x) } text("max iterations")        

    }
    
      if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("TopicModel")

      if (c.local)
        conf.setMaster("local")
        .set("spark.akka.frameSize", "300")

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
        val cu = new TopicModel(sc,c)
        cu.run()
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