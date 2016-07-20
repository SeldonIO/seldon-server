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
package io.seldon.stream.itemsim;

import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.JsonNode;

public class ItemSimilarityProcessActivity {
	
	final StreamingJaccardSimilarity streamJaccard;
	Timer outputTimer;
	long lastTime = 0;
	int window;
	String outputTopic;

	public ItemSimilarityProcessActivity(final Namespace ns)
	{
		this.window = ns.getInt("window_secs");
		this.outputTopic = ns.getString("output_topic");
		System.out.println(ns);
		this.streamJaccard = new StreamingJaccardSimilarity(window, ns.getInt("hashes"), ns.getInt("min_activity"));
		createOutputSimilaritiesTimer(ns);
	}
	
	public void createOutputSimilaritiesTimer(Namespace ns)
	{
		int windowSecs = ns.getInt("window_secs");
		int timer_ms = windowSecs * 1000;
		System.out.println("Scheduling at "+timer_ms);
		outputTimer = new Timer(true);
		outputTimer.scheduleAtFixedRate(new TimerTask() {
			   public void run()  
			   {
				   System.out.println("getting similarities");
				   List<JaccardSimilarity> res = streamJaccard.getSimilarity(System.currentTimeMillis()/1000);
				   System.out.println("Results size "+res.size());
				   for (JaccardSimilarity j : res)
					   System.out.println("Result:"+j.toString());
			   }
		   }, timer_ms, timer_ms);
	}
	
	
	@SuppressWarnings("unchecked")
	public void process(final Namespace ns) throws InterruptedException
	{
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-item-similarity");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ns.getString("kafka"));
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, ns.getString("zookeeper"));
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KStreamBuilder builder = new KStreamBuilder();
        
        JsonDeserializer jsonDeserializer = new JsonDeserializer();
        
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(),jsonDeserializer);
        //final Serde<String> stringSerde = Serdes.String();
        final String topic = ns.getString("topic");
        System.out.println("topic:"+topic);
        final String parseDateMethod = ns.getString("parse_date_method");
        KStream<byte[], JsonNode> source = builder.stream(Serdes.ByteArray(),jsonSerde,topic);

        source.filter(new Predicate<byte[], JsonNode>() {
			
			@Override
			public boolean test(byte[] key, JsonNode value) {
				//System.out.println(value);
				String client = value.get("client").asText();
				if (client.equals(ns.getString("client")))
					return true;
				else
					return false;
			}
		})
        .foreach(new ForeachAction<byte[], JsonNode>() {

			@Override
			public void apply(byte[] key, JsonNode value) {
				Long user = value.get("userid").asLong();
				Long item = value.get("itemid").asLong();
				Long time;
				if (parseDateMethod.equals("json-utc"))
				{
					//expected 2016-07-18T08:49:45Z
					DateTime dtime = new DateTime(value.get("timestamp_utc").asText());
					time = dtime.getMillis()/1000;
				}
				else if (parseDateMethod.equals("json-time"))
					time = value.get("time").asLong();
				else
					time = System.currentTimeMillis()/1000;
				
				//System.out.println("User:"+user+"item:"+item+"time:"+time);
				ItemSimilarityProcessActivity.this.streamJaccard.add(item, user, time);
				
				//debugging only
				if (ItemSimilarityProcessActivity.this.lastTime == 0)
					ItemSimilarityProcessActivity.this.lastTime = time;
				long diff = time - ItemSimilarityProcessActivity.this.lastTime;
				if (diff >= window)
				{
					System.out.println("getting similarities");
					List<JaccardSimilarity> res = streamJaccard.getSimilarity(time);
					System.out.println("Results size "+res.size());
					sendMessages(res, time);
					ItemSimilarityProcessActivity.this.lastTime = time;
				}
			}
		});
    	
               
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
       
	}
	
	
	public void sendMessages(List<JaccardSimilarity> sims,long timestamp)
	{
		  Properties producerConfig = new Properties();
		  producerConfig.put("bootstrap.servers", "localhost:9092");
		  producerConfig.put("key.serializer",
				  "org.apache.kafka.common" +
				  ".serialization.ByteArraySerializer");
		  producerConfig.put("value.serializer",
				  "org.apache.kafka.common" +
				  ".serialization.StringSerializer");
		  KafkaProducer producer = new KafkaProducer<byte[], String>(producerConfig);
		  StringBuffer buf = new StringBuffer();
		  for(JaccardSimilarity s : sims)
		  {
			  buf.append(timestamp).append(",").append(s.item1).append(",").append(s.item2).append(",").append(s.similarity);
			  producer.send(new ProducerRecord<byte[], String>(this.outputTopic, "A".getBytes(), buf.toString()));
			  buf.delete(0, buf.length());
		  }
		  

	}
	
    public static void main(String[] args) throws Exception {
        
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("ImpressionsToInfluxDb")
                .defaultHelp(true)
                .description("Read Seldon impressions and send stats to influx db");
    	parser.addArgument("-t", "--topic").setDefault("actions").help("Kafka topic to read from");
    	parser.addArgument("-c", "--client").required(true).help("Client to run item similarity");
    	parser.addArgument("-o", "--output-topic").required(true).help("Output topic");
    	parser.addArgument("-k", "--kafka").setDefault("localhost:9092").help("Kafka server and port");
    	parser.addArgument("-z", "--zookeeper").setDefault("localhost:2181").help("Zookeeper server and port");
    	parser.addArgument("-w", "--window-secs").type(Integer.class).setDefault(3600*5).help("streaming window size in secs");
    	parser.addArgument("--hashes").type(Integer.class).setDefault(100).help("number of hashes");
    	parser.addArgument("-m", "--min-activity").type(Integer.class).setDefault(200).help("min activity");
    	parser.addArgument("-p", "--parse-date-method").choices("json-time","json-utc","system").setDefault("json-time").help("min activity");
        
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
            ItemSimilarityProcessActivity processor = new ItemSimilarityProcessActivity(ns);
            processor.process(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }

}
