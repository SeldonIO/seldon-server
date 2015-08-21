package io.seldon.spark.events

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import io.seldon.spark.SparkUtils
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.columnar.BOOLEAN
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.storage.StorageLevel

case class ProcessEventsConfig(
        input_path_pattern: String = "",
        input_date_string: String = "",
        output_path_dir: String = "",
        aws_access_key_id: String = "",
        aws_secret_access_key: String = "",
        debug_use_local_master: Boolean = false,
        gzip_output: Boolean = false,
        dry_run: Boolean = false) {

    override def toString =
        "input_path_pattern[%s]\n".format(input_path_pattern) +
            "input_date_string[%s]\n".format(input_date_string) +
            "output_path_dir[%s]\n".format(output_path_dir) +
            "aws_access_key_id[%s]\n".format(aws_access_key_id) +
            "aws_secret_access_key[%s]\n".format(aws_secret_access_key) +
            "debug_use_local_master[%s]\n".format(debug_use_local_master) +
            "gzip_output[%s]\n".format(gzip_output) +
            "dry_run[%s]\n".format(dry_run) +
            ""
}

class ProcessEventsJob(private val sc: SparkContext, config: ProcessEventsConfig) {

    def parseJson(path: String) = {

        val rdd = sc.textFile(path).map( (line) => {
            import org.json4s._
            import org.json4s.jackson.JsonMethods._
            implicit val formats = DefaultFormats

            val parts = line.split("\\s+", 3)
            val event_json_line = if (parts.length == 3) {
                parts(2)
            } else {
                "{\"client\": UNKOWN}"
            }

            val json = parse(event_json_line)
            val client = (json \ "client").extract[String]

            (client, event_json_line)
        }).persist(StorageLevel.MEMORY_AND_DISK)

        rdd
    }

    def run() = {
        import io.seldon.spark.actions.JobUtils

        var unixDays = 0L;
        try {
            unixDays = JobUtils.dateToUnixDays(config.input_date_string);
        } catch {
            case ex: java.text.ParseException => {
                unixDays = 0L
            }
        }
        println("--- started ProcessEventsJob date[%s] unixDays[%s] ---".format(config.input_date_string, unixDays));
        println("Env: " + System.getenv())
        println("Properties: " + System.getProperties())
        ProcessEventsJob.dumpSparkConf(sc)
        ProcessEventsJob.dumpDumpConfig(config)
        
        val fileGlob = JobUtils.getSourceDirFromDate(config.input_path_pattern, config.input_date_string)

        val jsonRdd = parseJson(fileGlob).repartition(4)
        val clientList = jsonRdd.keys.distinct().collect()
        for (client <- clientList) {
            val outputPath = getOutputPath(config.output_path_dir, unixDays, client)
            val dryRunString = if (config.dry_run) "(DRY-RUN) " else ""
            println("%sProcessing client[%s] outputPath[%s]".format(dryRunString, client, outputPath))
            val filtered_by_client: RDD[(String, String)] = jsonRdd.filter((y) => {
                if (client.equals(y._1)) {
                    true
                } else {
                    false
                }
            })
            val client_rdd: RDD[String] = filtered_by_client.map((z) => z._2)

            if (config.dry_run == false) {
                saveClientRdd(client, client_rdd, outputPath, config.gzip_output)
            }
        }

        println("--- finished ProcessEventsJob date[%s] unixDays[%s] ---".format(config.input_date_string, unixDays))

    }

    def saveClientRdd(client: String, rdd: RDD[String], outputPath: String, gzip_output: Boolean) = {
        if (gzip_output) {
            rdd.saveAsTextFile(outputPath, classOf[GzipCodec])
        } else {
            rdd.saveAsTextFile(outputPath)
        }
    }

    def getOutputPath(output_path_dir: String, unixDays: Long, client: String) = {
        output_path_dir + "/" + client + "/events/" + unixDays;
    }

}

object ProcessEventsJob {

    def main(args: Array[String]) = {

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        var c = new ProcessEventsConfig()
        val parser = new scopt.OptionParser[Unit]("ProcessEventsJob") {
            head("ProcessEventsJob", "1.0")
            opt[String]("input-path-pattern") required () valueName ("input path") foreach { x => c = c.copy(input_path_pattern = x) } text ("input path")
            opt[String]("input-date-string") required () valueName ("input date string") foreach { x => c = c.copy(input_date_string = x) } text ("input date string")
            opt[String]("output-path-dir") required () valueName ("output path dir") foreach { x => c = c.copy(output_path_dir = x) } text ("output path dir")
            opt[String]("aws-access-key-id") valueName ("the aws_access_key_id") foreach { x => c = c.copy(aws_access_key_id = x) } text ("the aws_access_key_id")
            opt[String]("aws-secret-access-key") valueName ("the aws_secret_access_key") foreach { x => c = c.copy(aws_secret_access_key = x) } text ("the aws_secret_access_key")
            opt[Unit]("debug-use-local-master") foreach { x => c = c.copy(debug_use_local_master = true) } text ("use local master")
            opt[Unit]("gzip-output") foreach { x => c = c.copy(gzip_output = true) } text ("gzip output flag")
            opt[Unit]("dry-run") foreach { x => c = c.copy(dry_run = true) } text ("dry run flag")
        }

        if (parser.parse(args)) {
            val conf = new SparkConf().setAppName("ProcessEventsJob")

            if (c.debug_use_local_master)
                conf.setMaster("local")
            //       .set("spark.akka.frameSize", "300")

            val sc = new SparkContext(conf)
            try {
                sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
                if (c.aws_access_key_id.nonEmpty && c.aws_secret_access_key.nonEmpty) {
                    println("-- setting s3 access --")
                    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", c.aws_access_key_id)
                    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", c.aws_secret_access_key)
                }

                val cu = new ProcessEventsJob(sc, c)
                cu.run()
            } finally {
                println("Shutting down job")
                sc.stop()
            }
        } else {
            println("args parsing failure!")
        }
    }

    def dumpSparkConf(sc: SparkContext) = {
        println("--- sparkConf ---")
        val kv_pairs: Array[(String, String)] = sc.getConf.getAll
        for ((k, v) <- kv_pairs) {
            println("%s[%s]".format(k, v));
        }
        println("-----------------")
    }

    def dumpDumpConfig(c: ProcessEventsConfig) = {
        println("--- processEventsConfig ---")
        print(c)
        println("---------------------------")
    }

}
