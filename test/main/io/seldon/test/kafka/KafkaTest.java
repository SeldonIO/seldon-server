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

package io.seldon.test.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.junit.Test;

public class KafkaTest {


    @Test
    public void testSend() throws InterruptedException
    {

        Properties props = new Properties();
        props.put("broker.list", "0:localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("queue.size", "2");
        ProducerConfig config = new ProducerConfig(props);
		

		/*
		Properties props = new Properties();
		props.put("zk.connect", "127.0.0.1:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		*/
        // send a message using default partitioner
        Producer<String, String> producer = new Producer<>(config);

        for(int i=0;i<1;i++)
        {
            System.out.println("Sending..");

            KeyedMessage<String,String> data = new KeyedMessage<>("action_queue", "test-message-again"+i);
            producer.send(data);

            Thread.sleep(10000);
        }
    }

}
