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
package io.seldon.stream.analytics;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import com.fasterxml.jackson.databind.JsonNode;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ImpressionsToInfluxDb {
	
	private static Logger logger = Logger.getLogger(ImpressionsToInfluxDb.class.getName());
	
	@SuppressWarnings("unchecked")
	public static void process(final Namespace ns) throws InterruptedException
	{
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-impressions");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ns.getString("kafka"));
        //props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, ns.getString("zookeeper"));
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
		final InfluxDB influxDB = InfluxDBFactory.connect("http://"+ns.getString("influxdb"), ns.getString("influx_user"), ns.getString("influx_password"));
		influxDB.enableBatch(50, 5, TimeUnit.SECONDS);
        
        KStreamBuilder builder = new KStreamBuilder();
        
        JsonDeserializer jsonDeserializer = new JsonDeserializer();
        
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(),jsonDeserializer);
        final Serde<String> stringSerde = Serdes.String();
        
        io.seldon.stream.serializer.JsonSerializer<Impression> impressionJsonSerializer = new io.seldon.stream.serializer.JsonSerializer<>();
        io.seldon.stream.serializer.JsonDeserializer<Impression> impressionJsonDeserializer = new io.seldon.stream.serializer.JsonDeserializer<>(Impression.class);
        Serde<Impression> impressionSerde = Serdes.serdeFrom(impressionJsonSerializer,impressionJsonDeserializer);

        io.seldon.stream.serializer.JsonSerializer<Request> requestJsonSerializer = new io.seldon.stream.serializer.JsonSerializer<>();
        io.seldon.stream.serializer.JsonDeserializer<Request> requestJsonDeserializer = new io.seldon.stream.serializer.JsonDeserializer<>(Request.class);
        Serde<Request> requestSerde = Serdes.serdeFrom(requestJsonSerializer,requestJsonDeserializer);
        
        System.out.println("TOPIC is "+ns.getString("topic"));
        logger.info("Topic is "+ns.getString("topic"));
        KStream<String, JsonNode> source = builder.stream(stringSerde,jsonSerde,ns.getString("topic"));
     
        KStream<String,JsonNode>[] branches = source.branch(
        		new Predicate<String, JsonNode>()
        			{
        			@Override
        			public boolean test(String key, JsonNode value)
        			{
        				if (value.get("tag").asText().equals("restapi.ctralg"))
        				{
        					logger.info("found message with tag "+value.get("tag").asText());
        					return true;
        				}
        				else
        				{
        					return false;
        				}
        			}
        			},
        			new Predicate<String, JsonNode>()
        			{
        			@Override
        			public boolean test(String key, JsonNode value)
        			{
        				if (value.get("tag").asText().equals("restapi.calls"))
        				{
        					logger.info("found message with tag "+value.get("tag").asText());
        					return true;
        				}
        				else
        				{
        					return false;
        				}
        			}
        			}
        		);
        
        KStream<String,JsonNode> impressionsStream = branches[0];
        KStream<String,JsonNode> requestsStream = branches[1];
        
        /*
         * Impressions topology
         */
        
        StateStoreSupplier impressionsStore = Stores.create("impressionsStore")
                .withKeys(Serdes.String())
                .withValues(impressionSerde)
                .persistent()
                .windowed(1000, 5000, 2, false)
                .build();
        
        impressionsStream.map(new KeyValueMapper<String, JsonNode, KeyValue<String,Impression>>() {

			@Override
			public KeyValue<String, Impression> apply(String key, JsonNode value) {

				//Nasty hack until we get correct method to reduce and send non or final per second aggregations to influxdb
				Random r = new Random();
				Impression imp = new Impression(value);
				String ikey = imp.consumer+"_"+imp.rectag+"_"+imp.variation+"_"+imp.time;
				return new KeyValue<String,Impression>(ikey,imp);
			}
        	
		}).groupByKey(stringSerde,impressionSerde)
		.reduce(new Reducer<Impression>() {
			
			@Override
			public Impression apply(Impression value1, Impression value2) {
				return value1.add(value2);
			}
		},TimeWindows.of(1000).until(5000), impressionsStore)
		.foreach(
				new ForeachAction<String, Impression>() {
			@Override
			public void apply(String key, Impression value) {
			
				Random r = new Random();
				long time = value.time * 1000000;
				time = time + r.nextInt(1000000);
				
				Point point = Point.measurement(ns.getString("influx_measurement_impressions"))
                .time(time, TimeUnit.MICROSECONDS)
                .tag("client", value.consumer)
                .tag("rectag", value.rectag)
                .tag("variation", value.variation)
                .addField("impressions", value.imp)
                .addField("clicks", value.click)
                .build();
				
				logger.info(key+"Value is "+value.toString());
				influxDB.write(ns.getString("influx_database"), "default", point);				
			}
		});
		
        StateStoreSupplier requestsStore = Stores.create("requestStore")
                .withKeys(Serdes.String())
                .withValues(requestSerde)
                .persistent()
                .windowed(1000, 5000, 2, false)
                .build();
        
        requestsStream.map(new KeyValueMapper<String, JsonNode, KeyValue<String,Request>>() {

			@Override
			public KeyValue<String, Request> apply(String key, JsonNode value) {
				//Nasty hack until we get correct method to reduce and send non or final per second aggregations to influxdb
				Random r = new Random();

				Request req = new Request(value);
				String rkey = req.consumer+"_"+req.path+"_"+req.httpmethod+"_"+req.time;
				return new KeyValue<String,Request>(rkey,req);
			}
        	
		}).groupByKey(stringSerde,requestSerde)
		.reduce(new Reducer<Request>() {
			
			@Override
			public Request apply(Request value1, Request value2) {
				return value1.add(value2);
			}
		}, TimeWindows.of(1000).until(5000),requestsStore)
		.foreach(new ForeachAction<String, Request>() {
			
			@Override
			public void apply(String key, Request value) {
			

				Random r = new Random();
				long time = value.time * 1000000;
				time = time + r.nextInt(1000000);

				Point point = Point.measurement(ns.getString("influx_measurement_requests"))
                .time(time, TimeUnit.MICROSECONDS)
                .tag("client", value.consumer)
                .tag("path", value.path)
                .tag("httpmethod", value.httpmethod)
                .addField("count", value.count)
                .addField("exectime", value.exectime/((float)value.count))
                .build();

				
				logger.info("Value is "+value.toString());
				influxDB.write(ns.getString("influx_database"), "default", point);				
			}
		});
		
        
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        

	}
	
    public static void main(String[] args) throws Exception {
        
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("ImpressionsToInfluxDb")
                .defaultHelp(true)
                .description("Read Seldon impressions and send stats to influx db");
    	parser.addArgument("-t", "--topic").setDefault("impressions").help("Kafka topic to read from");
    	parser.addArgument("-k", "--kafka").setDefault("localhost:9092").help("Kafka server and port");
    	parser.addArgument("-z", "--zookeeper").setDefault("localhost:2181").help("Zookeeper server and port");
    	parser.addArgument("-i", "--influxdb").setDefault("localhost:8086").help("Influxdb server and port");
    	parser.addArgument("-u", "--influx-user").setDefault("root").help("Influxdb user");
    	parser.addArgument("-p", "--influx-password").setDefault("root").help("Influxdb password");
    	parser.addArgument("-d", "--influx-database").setDefault("seldon").help("Influxdb database");
    	parser.addArgument("--influx-measurement-impressions").setDefault("impressions").help("Influxdb impressions measurement");
    	parser.addArgument("--influx-measurement-requests").setDefault("requests").help("Influxdb requests measurement");
        
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
            ImpressionsToInfluxDb.process(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }

}
