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
package io.seldon.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Component;

@Component
public class SparkContextPeer {

	private SQLContext sqlContext;
	private JavaSparkContext sc;
	Pipeline p;
	
	public SparkContextPeer()
	{
		createContext();
	}
	
	private void createContext()
	{
		sc = new JavaSparkContext("local","Simple_App");
		sqlContext = new org.apache.spark.sql.SQLContext(sc);
		p = createPipeline();
	}

	
	private Pipeline createPipeline()
	{
		Tokenizer tokenizer = new Tokenizer()
		  .setInputCol("text")
		  .setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
		  .setNumFeatures(1000)
		  .setInputCol(tokenizer.getOutputCol())
		  .setOutputCol("features");
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[] {tokenizer, hashingTF});
		return pipeline;
	}
	
	public void test() throws InterruptedException
	{
		List<String> json = new ArrayList<>();
		json.add("{\"text\":\"some words\"}");
		JavaRDD<String> rdd = sc.parallelize(json);
		DataFrame df = sqlContext.read().format("json").json(rdd);
		//Pipeline p = createPipeline();
		PipelineModel m = p.fit(df);
		DataFrame r = m.transform(df);
		System.out.println(r);
		Row[] rows = r.select("features").collect();
		for (Row row : rows)
			System.out.println(row);
		
	}
	
	public static void main(String[] args) throws InterruptedException
	{
		
		final SparkContextPeer p = new SparkContextPeer();
		for(int i=0;i<1000;i++)
		{
			//Thread.sleep(200);
			Thread t = new Thread(new Runnable() {

				@Override
				public void run() {
					
					try {
						System.out.println(" start ");
						p.test();
						System.out.println(" end ");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			});
			
			t.start();

		}
	}
	
	
	
}
