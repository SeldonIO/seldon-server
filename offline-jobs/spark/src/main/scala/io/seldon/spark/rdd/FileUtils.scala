package io.seldon.spark.rdd

import java.util.zip.GZIPOutputStream

import org.apache.spark.rdd.RDD
import java.io._


 

object FileUtils {

 
 import DataSourceMode._
    
  def toSparkResource(location:String, mode:DataSourceMode): String = {
    mode match {
      case LOCAL => return location.replace("local:/","")
      case S3 => return location
    }

  }

  def toOutputResource(location:String, mode: DataSourceMode): String = {
    mode match {
      case LOCAL => return location.replace("local:/","")
      case S3 => return location.replace("s3n://", "")
    }
  }
  
  def outputModelToFile(model: RDD[String],outputFilesLocation:String, outputType:DataSourceMode,filename:String) {
    outputType match {
      case LOCAL => outputModelToLocalFile(model.collect(),outputFilesLocation,filename)
      case S3 => outputModelToS3File(model.collect(), toOutputResource(outputFilesLocation,outputType), filename)
    }
 }
  
  def outputModelToFile(lines: Array[String],outputFilesLocation:String, outputType:DataSourceMode,filename:String) {
    outputType match {
      case LOCAL => outputModelToLocalFile(lines,outputFilesLocation,filename)
      case S3 => outputModelToS3File(lines, toOutputResource(outputFilesLocation,outputType), filename)
    }
 }
  
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  
  def outputModelToLocalFile(lines: Array[String], outputFilesLocation: String, filename : String) = {
    new File(outputFilesLocation).mkdirs()
    val userFile = new File(outputFilesLocation+"/"+filename);
    userFile.createNewFile()
    printToFile(userFile){
      p => lines.foreach {
        s => {
          p.println(s)
        }
      }
    }
  }
  
  
   def outputModelToS3File(lines: Array[String], outputFilesLocation: String,  filename : String) = {
     import org.jets3t.service.S3Service
     import org.jets3t.service.impl.rest.httpclient.RestS3Service
     import org.jets3t.service.model.{S3Object, S3Bucket}
     import org.jets3t.service.security.AWSCredentials
     val service: S3Service = new RestS3Service(new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))
     val bucketString = outputFilesLocation.split("/")(0)
     val bucket = service.getBucket(bucketString)
     val s3Folder = outputFilesLocation.replace(bucketString+"/","")
     val outBuf = new StringBuffer()
     lines.foreach(u => {
      outBuf.append(u)
      outBuf.append("\n")
    })
     val obj = new S3Object(s3Folder+"/"+filename, outBuf.toString())
     service.putObject(bucket, obj)
  }

  def gzip(path: String):File = {
    val buf = new Array[Byte](1024)
    val src = new File(path)
    val dst = new File(path ++ ".gz")

    try {
      val in  = new BufferedInputStream(new FileInputStream(src))
      try {
        val out = new GZIPOutputStream(new FileOutputStream(dst))
        try {
          var n = in.read(buf)
          while (n >= 0) {
            out.write(buf, 0, n)
            n = in.read(buf)
          }
        }
        finally {
          out.flush
          out.close()
          in.close()
        }
      } catch {
        case _:FileNotFoundException =>
          System.err.printf("Permission Denied: %s", path ++ ".gz")
        case _:SecurityException =>
          System.err.printf("Permission Denied: %s", path ++ ".gz")
      }
    } catch {
      case _: FileNotFoundException =>
        System.err.printf("File Not Found: %s", path)
      case _: SecurityException =>
        System.err.printf("Permission Denied: %s", path)
    }
    return dst;
  }
  
}