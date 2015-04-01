/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.sv;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import pitt.search.semanticvectors.FlagConfig;
import pitt.search.semanticvectors.ObjectVector;
import pitt.search.semanticvectors.vectors.Vector;
import pitt.search.semanticvectors.vectors.VectorFactory;

public class VectorEnumerationText implements Enumeration<ObjectVector> {
    BufferedReader vecBuf;
    FlagConfig flagConfig;

    public VectorEnumerationText(BufferedReader vecBuf,FlagConfig flagConfig) {
      this.vecBuf = vecBuf;
      this.flagConfig = flagConfig;
    }

    /**
     * @return True if more vectors are available. False if vector
     * store is exhausted, including exceptions from reading past EOF.
     */
    public boolean hasMoreElements() {
      try {
        char[] cbuf = new char[1];
        vecBuf.mark(10);
        if (vecBuf.read(cbuf, 0, 1) != -1) {
          vecBuf.reset();
          return true;
        }
        else {
          vecBuf.close();
          return false;
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }

    /**
     * @return Next element if found.
     * @throws NoSuchElementException if no element is available.
     */
    public ObjectVector nextElement() throws NoSuchElementException {
      try {
        return parseVectorLine(vecBuf.readLine());
      }
      catch (IOException e) {
        e.printStackTrace();
        throw (new NoSuchElementException("Failed to get next element from vector store."));
      }
    }
    
    public ObjectVector parseVectorLine(String line) throws IOException {
        int firstSplitPoint = line.indexOf("|");
        String objectName = new String(line.substring(0, firstSplitPoint));
        Vector tmpVector = VectorFactory.createZeroVector(flagConfig.vectortype(), flagConfig.dimension());
        tmpVector.readFromString(line.substring(firstSplitPoint + 1, line.length()));
        return new ObjectVector(objectName, tmpVector);
      }
}
