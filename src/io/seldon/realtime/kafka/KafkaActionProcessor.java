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

package io.seldon.realtime.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import io.seldon.realtime.IActionProcessor;

public class KafkaActionProcessor implements IActionProcessor {

	Producer<String, String> producer;
	String host;
	int port;
	String qName;
	
	
	
	public KafkaActionProcessor(String host, int port, String qName) {
		this.host = host;
		this.port = port;
		this.qName = qName;
		this.createProducer();
	}
	
	private void createProducer()
	{
		Properties props = new Properties();
		props.put("broker.list", "0:"+host+":"+port);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		//props.put("queue.size", "10000");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<>(config);
	}



	@Override
	public void addAction(long userId, long itemId, double value, long time,int type) {
        //List<String> messages = new java.util.ArrayList<String>();
        //messages.add(""+userId+","+itemId+","+value+","+time+","+type);
        String message = ""+userId+","+itemId+","+value+","+time+","+type;
        //ProducerData<String, String> data = new ProducerData<String, String>(qName, messages);
        KeyedMessage<String,String> data = new KeyedMessage<>(qName, message);

        producer.send(data);
	}

}
