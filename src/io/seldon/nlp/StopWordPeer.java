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

package io.seldon.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StopWordPeer {
	
	private static StopWordPeer thePeer = null;
	
	Set<String> stopWords;
	
	public static StopWordPeer initialise(String path)
	{
		thePeer = new StopWordPeer(path);
		return thePeer;
	}
	
	public static StopWordPeer get()
	{
		return thePeer;
	}
	
	private StopWordPeer(String path)
	{
		loadEnglishStopWords(path);
	}
	
	private void loadEnglishStopWords(String path)
	{
		stopWords = new HashSet<String>();
        File dictFile = new File(path);
        BufferedReader input = null;
    	try {
    	  //use buffering
    	  //this implementation reads one line at a time
    	  input = new BufferedReader( new FileReader(dictFile) );
    	  String line = null; //not declared within while loop
    	  while (( line = input.readLine()) != null)
    	  {
    		stopWords.add(line.toLowerCase().trim());
    	  }
    	}
    	catch (FileNotFoundException ex) {
    	  System.out.println("File not found " + dictFile.toString());
    	  return;
    	}
    	catch (IOException ex){
    		System.out.println("IO Exception on reading file " + dictFile.toString());
    		return;
    	}
    	finally
    	{
    		if (input != null)
    		{
				try 
				{
					input.close();
				} catch (IOException e) 
				{
		    		System.out.println("IO Exception on reading file " + dictFile.toString());
				}
    		}
    	}
	}
	
	public String removeStopWords(String phrase)
	{
		if (phrase != null && !"".equals(phrase))
		{
			phrase = phrase.trim().toLowerCase();
			String parts[] = phrase.split("\\s+");
			StringBuffer buf = new StringBuffer();
			for(int i=0;i<parts.length;i++)
				if (!stopWords.contains(parts[i]))
					buf.append(parts[i]).append(" ");
			return buf.toString().trim();
		}
		else
			return null;
	}
}
