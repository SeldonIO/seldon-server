/*
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
package io.seldon.spark.features

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class SeldonWord2VecModel (
    private val model: Map[String, Array[Float]]) extends Serializable {

  private def cosineSimilarity(v1: Array[Float], v2: Array[Float]): Double = {
    require(v1.length == v2.length, "Vectors should have the same length")
    val n = v1.length
    val norm1 = blas.snrm2(n, v1, 1)
    val norm2 = blas.snrm2(n, v2, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.sdot(n, v1, 1, v2,1) / norm1 / norm2
  }
  
  /**
   * Transforms a word to its vector representation
   * @param word a word 
   * @return vector representation of word
   */
  def transform(word: String): Vector = {
    model.get(word) match {
      case Some(vec) =>
        Vectors.dense(vec.map(_.toDouble))
      case None =>
        throw new IllegalStateException(s"$word not in vocabulary")
    }
  }

  /**
   * Find synonyms of a word
   * @param word a word
   * @param num number of synonyms to find  
   * @return array of (word, cosineSimilarity)
   */
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = transform(word)
    findSynonyms(vector,num)
  }

  /**
   * Find synonyms of the vector representation of a word
   * @param vector vector representation of a word
   * @param num number of synonyms to find  
   * @return array of (word, cosineSimilarity)
   */
  def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    // TODO: optimize top-k
    val fVector = vector.toArray.map(_.toFloat)
    model.mapValues(vec => cosineSimilarity(fVector, vec))
      .toSeq
      .sortBy(- _._2)
      .take(num + 1)
      .tail
      .toArray
  }
  
  /**
   * Returns a map of words to their vector representations.
   */
  def getVectors: Map[String, Array[Float]] = {
    model
  }
}