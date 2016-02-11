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
package io.seldon.spark.actions;

import java.text.ParseException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GroupActionsJob {

    public static class ClientDetail {
        public final String client;
        public final long itemCount;

        public ClientDetail(String client, long itemCount) {
            this.client = client;
            this.itemCount = itemCount;
        }
    }

    public static class CmdLineArgs {
        @Parameter(names = "--input-path-pattern", required = true)
        private String input_path_pattern;

        @Parameter(names = "--input-date-string", required = true)
        private String input_date_string;

        @Parameter(names = "--output-path-dir", required = true)
        private String output_path_dir;

        @Parameter(names = "--debug-use-local-master")
        private Boolean debug_use_local_master = false;

        @Parameter(names = "--aws-access-key-id", required = false)
        private String aws_access_key_id;

        @Parameter(names = "--aws-secret-access-key", required = false)
        private String aws_secret_access_key;

        @Parameter(names = "--gzip-output", required = false)
        private boolean gzip_output = false;

        @Override
		public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    public static void main(String[] args) {
        CmdLineArgs cmdLineArgs = new CmdLineArgs();
        new JCommander(cmdLineArgs, args);

        run(cmdLineArgs);
    }

    public static void run(CmdLineArgs cmdLineArgs) {
        long unixDays = 0;
        try {
            unixDays = JobUtils.dateToUnixDays(cmdLineArgs.input_date_string);
        } catch (ParseException e) {
            unixDays = 0;
        }
        System.out.println(String.format("--- started GroupActionsJob date[%s] unixDays[%s] ---", cmdLineArgs.input_date_string, unixDays));

        System.out.println("Env: " + System.getenv());
        System.out.println("Properties: " + System.getProperties());

        SparkConf sparkConf = new SparkConf().setAppName("GroupActionsJob");

        if (cmdLineArgs.debug_use_local_master) {
            System.out.println("Using 'local' master");
            sparkConf.setMaster("local");
        }

        Tuple2<String, String>[] sparkConfPairs = sparkConf.getAll();
        System.out.println("--- sparkConf ---");
        for (int i = 0; i < sparkConfPairs.length; i++) {
            Tuple2<String, String> kvPair = sparkConfPairs[i];
            System.out.println(String.format("%s:%s", kvPair._1, kvPair._2));
        }
        System.out.println("-----------------");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        { // setup aws access
            Configuration hadoopConf = jsc.hadoopConfiguration();
            hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
            if (cmdLineArgs.aws_access_key_id != null && !"".equals(cmdLineArgs.aws_access_key_id))
            {
            	hadoopConf.set("fs.s3n.awsAccessKeyId", cmdLineArgs.aws_access_key_id);
            	hadoopConf.set("fs.s3n.awsSecretAccessKey", cmdLineArgs.aws_secret_access_key);
            }
        }

        // String output_path_dir = "./out/" + input_date_string + "-" + UUID.randomUUID();

        JavaRDD<String> dataSet = jsc.textFile(JobUtils.getSourceDirFromDate(cmdLineArgs.input_path_pattern, cmdLineArgs.input_date_string)).repartition(4);

    	final ObjectMapper objectMapper = new ObjectMapper();
    	objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JavaPairRDD<String, ActionData> pairs = dataSet.mapToPair(new PairFunction<String, String, ActionData>() {

            @Override
            public Tuple2<String, ActionData> call(String t) throws Exception {
                ActionData actionData = JobUtils.getActionDataFromActionLogLine(objectMapper, t);
                // String key = (actionData.userid == 0) ? "__no_userid__" : actionData.client;
                String key = actionData.client;
                return new Tuple2<String, ActionData>(key, actionData);
            }

        }).persist(StorageLevel.MEMORY_AND_DISK());

        List<String> clientList = pairs.keys().distinct().collect();
        Queue<ClientDetail> clientDetailQueue = new PriorityQueue<ClientDetail>(30, new Comparator<ClientDetail>() {

            @Override
            public int compare(ClientDetail o1, ClientDetail o2) {
                if (o1.itemCount > o2.itemCount) {
                    return -1;
                } else if (o1.itemCount < o2.itemCount) {
                    return 1;
                }
                return 0;
            }
        });
        Queue<ClientDetail> clientDetailZeroQueue = new PriorityQueue<ClientDetail>(30, new Comparator<ClientDetail>() {

            @Override
            public int compare(ClientDetail o1, ClientDetail o2) {
                if (o1.itemCount > o2.itemCount) {
                    return -1;
                } else if (o1.itemCount < o2.itemCount) {
                    return 1;
                }
                return 0;
            }
        });
        System.out.println("Client list "+clientList.toString());
        for (String client : clientList) 
        {
        	if (client != null)
        	{
        	System.out.println("looking at client "+client);
            final String currentClient = client;

            JavaPairRDD<String, ActionData> filtered_by_client = pairs.filter(new Function<Tuple2<String, ActionData>, Boolean>() {

                @Override
                public Boolean call(Tuple2<String, ActionData> v1) throws Exception {
                    if (currentClient.equalsIgnoreCase(v1._1)) {
                        return Boolean.TRUE;
                    } else {
                        return Boolean.FALSE;
                    }
                }
            });

            JavaPairRDD<String, ActionData> nonZeroUserIds = filtered_by_client.filter(new Function<Tuple2<String, ActionData>, Boolean>() {

                @Override
                public Boolean call(Tuple2<String, ActionData> v1) throws Exception {
                    if (v1._2.userid == 0) {
                        return Boolean.FALSE;
                    } else {
                        return Boolean.TRUE;
                    }
                }
            });

            JavaPairRDD<String, Integer> userIdLookupRDD = nonZeroUserIds.mapToPair(new PairFunction<Tuple2<String, ActionData>, String, Integer>() {

                @Override
                public Tuple2<String, Integer> call(Tuple2<String, ActionData> t) throws Exception {
                    String key = currentClient + "_" + t._2.client_userid;
                    return new Tuple2<String, Integer>(key, t._2.userid);
                }
            });

            Map<String, Integer> userIdLookupMap = userIdLookupRDD.collectAsMap();
            Map<String, Integer> userIdLookupMap_wrapped = new HashMap<String, Integer>(userIdLookupMap);
            final Broadcast<Map<String, Integer>> broadcastVar = jsc.broadcast(userIdLookupMap_wrapped);
            JavaRDD<String> json_only_with_zeros = filtered_by_client.map(new Function<Tuple2<String, ActionData>, String>() {

                @Override
                public String call(Tuple2<String, ActionData> v1) throws Exception {
                    Map<String, Integer> m = broadcastVar.getValue();
                    ActionData actionData = v1._2;
                    if (actionData.userid == 0) {
                        String key = currentClient + "_" + actionData.client_userid;
                        if (m.containsKey(key)) {
                            actionData.userid = m.get(key);
                        } else {
                            return "";
                        }
                    }
                    String json = JobUtils.getJsonFromActionData(actionData);
                    return json;
                }
            });

            JavaRDD<String> json_only = json_only_with_zeros.filter(new Function<String, Boolean>() {

                @Override
                public Boolean call(String v1) throws Exception {
                    return (v1.length() == 0) ? Boolean.FALSE : Boolean.TRUE;
                }
            });

            String outputPath = getOutputPath(cmdLineArgs.output_path_dir, unixDays, client);
            if (cmdLineArgs.gzip_output) {
                json_only.saveAsTextFile(outputPath, org.apache.hadoop.io.compress.GzipCodec.class);
            } else {
                json_only.saveAsTextFile(outputPath);
            }
            long json_only_count = json_only.count();
            clientDetailZeroQueue.add(new ClientDetail(currentClient, json_only_with_zeros.count() - json_only_count));
            clientDetailQueue.add(new ClientDetail(currentClient, json_only_count));
        	}
        	else
        		System.out.println("Found null client!");
        }

        System.out.println("- Client Action (Zero Userid) Count -");
        while (clientDetailZeroQueue.size() != 0) {
            GroupActionsJob.ClientDetail clientDetail = clientDetailZeroQueue.remove();
            System.out.println(String.format("%s: %d", clientDetail.client, clientDetail.itemCount));
        }

        System.out.println("- Client Action Count -");
        while (clientDetailQueue.size() != 0) {
            GroupActionsJob.ClientDetail clientDetail = clientDetailQueue.remove();
            System.out.println(String.format("%s: %d", clientDetail.client, clientDetail.itemCount));
        }

        jsc.stop();
        System.out.println(String.format("--- finished GroupActionsJob date[%s] unixDays[%s] ---", cmdLineArgs.input_date_string, unixDays));

    }

    public static String getOutputPath(String output_path_dir, long unixDays, String client) {

        return output_path_dir + "/" + client + "/actions/" + unixDays;
    }
}
