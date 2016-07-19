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

import java.util.Properties;
import java.util.Random;

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
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.fasterxml.jackson.databind.JsonNode;

public class ItemSimilarityProcessActivity {
	
	@SuppressWarnings("unchecked")
	public static void process(final Namespace ns) throws InterruptedException
	{
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-impressions");
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
        final Serde<String> stringSerde = Serdes.String();
        final String topic = "fake-data5";
        
        KStream<byte[], JsonNode> source = builder.stream(Serdes.ByteArray(),jsonSerde,topic);
     
        source.foreach(new ForeachAction<byte[], JsonNode>() {

			@Override
			public void apply(byte[] key, JsonNode value) {
				Long user = value.get("user").asLong();
				Long item = value.get("item").asLong();
				Long time = value.get("time").asLong();
				System.out.println("User:"+user+"item:"+item+"time:"+time);
			}
		});
    	
       
		
		
		
		
   
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        
     // Now generate the data and write to the topic.
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("key.serializer",
                           "org.apache.kafka.common" +
                           ".serialization.ByteArraySerializer");
        producerConfig.put("value.serializer",
                           "org.apache.kafka.common" +
                           ".serialization.StringSerializer");
        KafkaProducer producer = 
            new KafkaProducer<byte[], String>(producerConfig);
        
        Random rng = new Random(12345L);
        
        int i = 0;
        while(true) {
        	
    		Long time = System.currentTimeMillis() / 1000;
    		Integer item = rng.nextInt(100);
    		Integer user = rng.nextInt(100);
    		String msg = "{\"user\":"+user+",\"item\":"+item+",\"time\":"+time+"}";
    		System.out.println(msg);
            producer.send(new ProducerRecord<byte[], String>(
                topic, "A".getBytes(), msg));
            i++;
            Thread.sleep(500L);
        } // Close infinite loop generating data.
        
        

	}
	
    public static void main(String[] args) throws Exception {
        
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("ImpressionsToInfluxDb")
                .defaultHelp(true)
                .description("Read Seldon impressions and send stats to influx db");
    	//parser.addArgument("-t", "--topic").setDefault("impressions").help("Kafka topic to read from");
    	parser.addArgument("-k", "--kafka").setDefault("localhost:9092").help("Kafka server and port");
    	parser.addArgument("-z", "--zookeeper").setDefault("localhost:2181").help("Zookeeper server and port");
        
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
            ItemSimilarityProcessActivity.process(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }

}
