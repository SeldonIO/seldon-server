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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import com.fasterxml.jackson.databind.JsonNode;

public class PredictionsToInfluxDb {
	@SuppressWarnings("unchecked")
	public static void process(final Namespace ns) throws InterruptedException
	{
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-predictions");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ns.getString("kafka"));
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, ns.getString("zookeeper"));
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
        
        io.seldon.stream.serializer.JsonSerializer<Prediction> predictionJsonSerializer = new io.seldon.stream.serializer.JsonSerializer<>();
        io.seldon.stream.serializer.JsonDeserializer<Prediction> predictionJsonDeserializer = new io.seldon.stream.serializer.JsonDeserializer<>(Prediction.class);
        Serde<Prediction> predictionSerde = Serdes.serdeFrom(predictionJsonSerializer,predictionJsonDeserializer);

        System.out.println("Topic is "+ns.getString("topic"));
        KStream<String, JsonNode> source = builder.stream(stringSerde,jsonSerde,ns.getString("topic"));
     
       
        
        source.filter(
        		new Predicate<String, JsonNode>()
    			{
    			@Override
    			public boolean test(String key, JsonNode value)
    			{
    				System.out.println("checking tag of "+value.get("tag").asText());
    				if (value.get("tag").asText().equals("predict.live"))
    				{
    					return true;
    				}
    				else
    				{
    					return false;
    				}
    			}
    			}
        		)
        .map(new KeyValueMapper<String, JsonNode, KeyValue<String,Prediction>>() {

			@Override
			public KeyValue<String, Prediction> apply(String key, JsonNode value) {
				//Nasty hack until we get correct method to reduce and send non or final per second aggregations to influxdb
				Random r = new Random();
				Prediction pred = new Prediction();
				pred.parse(value);
				String ikey = pred.consumer+"_"+pred.variation+"_"+pred.model+"_"+pred.predictedClass+"_"+pred.time+"_"+r.nextInt();;
				return new KeyValue<String,Prediction>(ikey,pred);
			}
        	
		}).groupByKey()
		.reduce(new Reducer<Prediction>() {
			
			@Override
			public Prediction apply(Prediction value1, Prediction value2) {
				return value1.add(value2);
			}
		}, "predReducer")
		.foreach(new ForeachAction<String, Prediction>() {
			
			@Override
			public void apply(String key, Prediction value) {
			
				Random r = new Random();
				long time = value.time * 1000000;
				time = time + r.nextInt(1000000);
				System.out.println("Value is "+value.toString());
				Point point = Point.measurement(ns.getString("influx_measurement"))
                .time(time, TimeUnit.MICROSECONDS)
                .tag("client", value.consumer)
                .tag("variation", value.variation)
                .tag("model", value.model)
                .tag("class",value.predictedClass)
                .addField("score", value.score/(double) value.count)
                .addField("count", value.count)
                .build();

				
				
				influxDB.write(ns.getString("influx_database"), "default", point);				
			}
		});
		
        

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        
	}
	
    public static void main(String[] args) throws Exception {
        
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("PredictionsToInfluxDb")
                .defaultHelp(true)
                .description("Read Seldon predictions and send stats to influx db");
    	parser.addArgument("-t", "--topic").setDefault("Predictions").help("Kafka topic to read from");
    	parser.addArgument("-k", "--kafka").setDefault("localhost:9092").help("Kafka server and port");
    	parser.addArgument("-z", "--zookeeper").setDefault("localhost:2181").help("Zookeeper server and port");
    	parser.addArgument("-i", "--influxdb").setDefault("localhost:8086").help("Influxdb server and port");
    	parser.addArgument("-u", "--influx-user").setDefault("root").help("Influxdb user");
    	parser.addArgument("-p", "--influx-password").setDefault("root").help("Influxdb password");
    	parser.addArgument("-d", "--influx-database").setDefault("seldon").help("Influxdb database");
    	parser.addArgument("--influx-measurement").setDefault("predictions").help("Influxdb Predictions measurement");
        
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
            PredictionsToInfluxDb.process(ns);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }
}
