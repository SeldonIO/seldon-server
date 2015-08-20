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

case class ProcessEventsConfig(
        input_path_pattern: String = "",
        input_date_string: String = "",
        output_path_dir: String = "",
        aws_access_key_id: String = "",
        aws_secret_access_key: String = "",
        debug_use_local_master: Boolean = false,
        gzip_output: Boolean = false) {

    override def toString =
        "input_path_pattern[%s]\n".format(input_path_pattern) +
            "input_date_string[%s]\n".format(input_date_string) +
            "output_path_dir[%s]\n".format(output_path_dir) +
            "aws_access_key_id[%s]\n".format(aws_access_key_id) +
            "aws_secret_access_key[%s]\n".format(aws_secret_access_key) +
            "debug_use_local_master[%s]\n".format(debug_use_local_master) +
            "gzip_output[%s]\n".format(gzip_output) +
            ""
}

class ProcessEventsJob(private val sc: SparkContext, config: ProcessEventsConfig) {

    def parseJson(path: String) = {

        val rdd = sc.textFile(path).map { line =>
            import org.json4s._
            import org.json4s.jackson.JsonMethods._
            implicit val formats = DefaultFormats

            val json = parse(line)
            val client = (json \ "client").extract[String]

            (client, line)

        }

        rdd
    }

    def run() = {
        //val fileGlob = config.inputPath + "/" + "/actions/" + SparkUtils.getS3UnixGlob(config.startDay, config.days) + "/*"
        val fileGlob = ""

        val jsonRdd = parseJson(fileGlob)

        /*
     * Do stuff
     */

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

                dumpSparkConf(sc)
                dumpDumpConfig(c)

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
