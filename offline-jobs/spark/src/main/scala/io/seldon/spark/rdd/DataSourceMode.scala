package io.seldon.spark.rdd

object DataSourceMode extends Enumeration {
    def fromString(s: String): DataSourceMode = {
      if(s.startsWith("/") || s.startsWith("local:/"))
        return LOCAL
      if(s.startsWith("s3n://"))
        return S3
      return NONE
    }
    type DataSourceMode = Value
    val S3, LOCAL, NONE = Value
  }