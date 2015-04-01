/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.spark

import org.joda.time.DateTime
import org.joda.time.Period

object SparkUtils {
  
 def getS3UnixGlob(start : Long,steps : Int): String = {
    val buf = new StringBuilder(100)
    buf ++= "{"
    for(i <- 0 to steps-1)
    {
      if (i> 0) buf ++= ","
      buf.append((start - i).toString())
    }
    buf ++= "}"
    buf.toString()
}
 
 def dateRange(start: DateTime, end: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(start)(_.plus(step)).takeWhile(!_.isAfter(end))
 
 def getS3UnixGlob(start : DateTime,end : DateTime) : String = {
   val range = dateRange(start,end,Period.days(1))
    val buf = new StringBuilder(100)
    buf ++= "{"
    while (range.hasNext)
    {
      val d = range.next()
      if (d.getMonthOfYear() < 10)
        buf ++= "0"
      buf.append(d.getMonthOfYear().toString())
      if (d.getDayOfMonth() < 10)
        buf ++= "0"
      buf.append(d.getDayOfMonth().toString())
      if (range.hasNext)
        buf ++= ","      
    }
   buf ++= "}"
   buf.toString()
 }
 
}