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

package io.seldon.test.realtime;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import backtype.storm.spout.KestrelClient;
import backtype.storm.spout.KestrelClient.Item;
import backtype.storm.spout.KestrelClient.ParseError;

import io.seldon.realtime.IActionProcessor;
import io.seldon.realtime.kestrel.KestrelActionProcessor;

public class ActionProcessorTest {

    String host = "localhost";
    int kestrelPort = 22133;
    String kestrelQName = "test";


    String kafkaTopic = "action_queue";
    int kafkaPort = 9092;

    private void kestrelMessageTest(IActionProcessor actionProcessor) throws IOException, ParseError
    {
        Long user = 1L;
        Long item = 2L;
        Double value = 0.34D;
        Long time = 4333L;
        Integer type = 1;
        actionProcessor.addAction(user,item,value,time,type);

        KestrelClient client = new KestrelClient(host,kestrelPort);
        Item i = client.dequeue(kestrelQName);
        Assert.assertNotNull(i);
        client.ack(kestrelQName, i._id);
        String iStr = new String(i._data, "UTF-8");
        Assert.assertNotNull(iStr);
        String parts[] = iStr.split(",");
        Assert.assertEquals(4, parts.length);
        Long _user = Long.parseLong(parts[0]);
        Long _item = Long.parseLong(parts[1]);
        Double _value = Double.parseDouble(parts[2]);
        Long _time = Long.parseLong(parts[3]);
        Integer _type = Integer.parseInt(parts[4]);
        Assert.assertEquals(user, _user);
        Assert.assertEquals(item, _item);
        Assert.assertEquals(value, _value);
        Assert.assertEquals(time, _time);
        Assert.assertEquals(type, _type);
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    /*

    public static class KafkaMessageConsumer implements Runnable
    {
        KafkaMessageStream<Message> stream;
        int processed = 0;
        String lastMessage;

        public KafkaMessageConsumer(KafkaMessageStream<Message> stream) {
            super();
            this.stream = stream;
        }



        public int getProcessed() {
            return processed;
        }



        public String getLastMessage() {
            return lastMessage;
        }



        @Override
        public void run()
        {
             for(Message message: stream)
             {
                  String val;
                try {
                    val = new String(toByteArray(message.payload()), "UTF-8");
                    System.out.println("consumed: " + val);
                    processed++;
                    lastMessage = val;
                } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
             }

        }
    }


    public void kafkaMessageTestHighLevel(IActionProcessor actionProcessor) throws InterruptedException
    {
        Long user = 1L;
        Long item = 2L;
        Double value = 0.34D;
        Long time = 4333L;
        Integer type = 1;
        actionProcessor.addAction(user,item,value,time,1);

        System.out.println("Waiting for message to be sent...");
        Thread.sleep(10000);


        // specify some consumer properties
        Properties props = new Properties();
        props.put("zk.connect", "localhost:2181");
        props.put("zk.connectiontimeout.ms", "1000000");
        props.put("groupid", "testing_group");

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        // create 4 partitions of the stream for topic “test”, to allow 4 threads to consume
        Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
            consumerConnector.createMessageStreams(ImmutableMap.of(kafkaTopic, 1));
        List<KafkaMessageStream<Message>> streams = topicMessageStreams.get(kafkaTopic);

        // create list of 4 threads to consume from each of the partitions
        ExecutorService executor = Executors.newFixedThreadPool(1);

        KafkaMessageConsumer c = null;
        // consume the messages in the threads
        for(final KafkaMessageStream<Message> stream: streams)
        {
            c = new KafkaMessageConsumer(stream);
            executor.submit(c);
        }

        Assert.assertNotNull(c);

        System.out.println("Waiting for message to be processed..");
        Thread.sleep(10000);

        Assert.assertEquals(1, c.getProcessed());

        consumerConnector.shutdown();

        String val = c.getLastMessage();
        Assert.assertNotNull(val);
        String parts[] = val.split(",");
        Assert.assertEquals(5, parts.length);
        Long _user = Long.parseLong(parts[0]);
        Long _item = Long.parseLong(parts[1]);
        Double _value = Double.parseDouble(parts[2]);
        Long _time = Long.parseLong(parts[3]);
        Integer _type = Integer.parseInt(parts[4]);
        Assert.assertEquals(user, _user);
        Assert.assertEquals(item, _item);
        Assert.assertEquals(value, _value);
        Assert.assertEquals(time, _time);
        Assert.assertEquals(type, _type);
    }


    public void kafkaMessageTest(IActionProcessor actionProcessor) throws UnsupportedEncodingException, InterruptedException
    {
        Long user = 1L;
        Long item = 2L;
        Double value = 0.34D;
        Long time = 4333L;
        Integer type = 1;
        actionProcessor.addAction(user,item,value,time,type);

        Thread.sleep(10000);

        // create a consumer to connect to the kafka server running on localhost, port 9092, socket timeout of 10 secs, socket receive buffer of ~1MB
        SimpleConsumer consumer = new SimpleConsumer(host, kafkaPort, 10000, 1024000);



        long offset = 0;
        // create a fetch request for topic “test”, partition 0, current offset, and fetch size of 1MB
        FetchRequest fetchRequest = new FetchRequest(kafkaTopic, 0, offset, 1000000);

        // get the message set from the consumer and print them out
        ByteBufferMessageSet messages = consumer.fetch(fetchRequest);

        Assert.assertTrue(messages.iterator().hasNext());


        MessageAndOffset msg = messages.iterator().next();

        String val = new String(toByteArray(msg.message().payload()), "UTF-8");
        System.out.println("consumed: " + val);
        // advance the offset after consuming each message
        offset = msg.offset();


        Assert.assertNotNull(val);
        String parts[] = val.split(",");
        Assert.assertEquals(4, parts.length);
        Long _user = Long.parseLong(parts[0]);
        Long _item = Long.parseLong(parts[1]);
        Double _value = Double.parseDouble(parts[2]);
        Long _time = Long.parseLong(parts[3]);
        Integer _type = Integer.parseInt(parts[4]);
        Assert.assertEquals(user, _user);
        Assert.assertEquals(item, _item);
        Assert.assertEquals(value, _value);
        Assert.assertEquals(time, _time);
        Assert.assertEquals(type, _type);
    }
    */
    // Needs a local kestrel server running
    @Ignore
    @Test
    public void testKestrelMessageSend() throws IOException, ParseError
    {
        IActionProcessor actionProcessor = new KestrelActionProcessor(kestrelQName, host,kestrelPort);
        kestrelMessageTest(actionProcessor);
    }



}
