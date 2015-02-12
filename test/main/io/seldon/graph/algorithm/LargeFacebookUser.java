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

package io.seldon.graph.algorithm;

import java.io.*;

/**
 * @author philipince
 *         Date: 16/12/2013
 *         Time: 15:28
 */
public class LargeFacebookUser {
    public static int[][] read(){
    try{
        InputStream in = LargeFacebookUser.class.getResourceAsStream("/LargeFacebookUser.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        int lines = 0;
        while (br.readLine() != null) lines++;
        in = LargeFacebookUser.class.getResourceAsStream("/LargeFacebookUser.txt");
        br = new BufferedReader(new InputStreamReader(in));
        int[][] toReturn = new int[lines][];
        String strLine;
        int lineNumber = 0;
        while ((strLine = br.readLine()) != null)   {

            // Print the content on the console
            String[] strings = strLine.split(",");
            int[] ints = new int[strings.length];
            for(int i = 0; i< strings.length; i++){
                ints[i]=Integer.parseInt(strings[i]);
            }
            toReturn[lineNumber] = ints;
            lineNumber++;
        }

        br.close();
        return toReturn;
    } catch (IOException e){
        e.printStackTrace();
        return null;
    }
    }

    public static void convertToChaco(){
        int[][] bareGraph = read();
        int total = 0;
        for(int[] line  : bareGraph){
            total += line.length;
        }
        System.out.println("original has " + bareGraph.length + " verices and "+ total + " edges");
        for(int i = 0; i < bareGraph.length; i++){
            for(int j : bareGraph[i]){
               boolean contains = false;

                for(int k : bareGraph[j]){
                    if(k==i){
                        contains = true;
                        break;
                    }
                }
                if(!contains) bareGraph[j] = addToArrayEnd(bareGraph[j], i);
            }
        }
        total = 0;
        for(int[] line  : bareGraph){
            total += line.length;
        }
        System.out.println("new has " + bareGraph.length + " verices and "+ total + " edges");
        StringBuffer buffer = new StringBuffer();
        for(int[] line : bareGraph){
            for(int j=0 ; j < line.length; j++){
                line[j]+=1;
                buffer.append(line[j]);
                if(j != (line.length-1)){
                    buffer.append(",");
                }
            }
            buffer.append('\n');
        }
        System.out.println(buffer);
    }


    public static int[] addToArrayEnd(int[] array, int number){
            int[] anotherArray = new int[array.length + 1];
            System.arraycopy(array, 0, anotherArray, 0, array.length);
            anotherArray[array.length] = number;
            return anotherArray;

    }

    public static void main(String[] args){
        convertToChaco();
    }
}
