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
package io.seldon.vw;

import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class VwModelManager extends ModelManager<VwModelManager.VwModel> {


    private static Logger logger = Logger.getLogger(VwModelManager.class.getName());
    private final ExternalResourceStreamer featuresFileHandler;
    private static final String MF_NEW_LOC_PATTERN = "vw";


    @Autowired
    public VwModelManager(ExternalResourceStreamer featuresFileHandler,
                          NewResourceNotifier notifier) {
        super(notifier, Collections.singleton(MF_NEW_LOC_PATTERN));
        this.featuresFileHandler = featuresFileHandler;

    }

    private Map<Integer, String> loadClassIdMap(BufferedReader reader) throws IOException {
        Map<Integer, String> classIdMap = new HashMap<Integer, String>();
        String line;
        while ((line = reader.readLine()) != null) {
            int firstComma = line.indexOf(",");
            if (firstComma > 0) {
                int id = Integer.parseInt(line.substring(0, firstComma));
                String className = line.substring(firstComma + 1);
                classIdMap.put(id, className);
            }
        }
        return classIdMap;
    }



    private VwModel loadModel(BufferedReader reader, Map<Integer, String> classIdMap) throws IOException {
        Map<Integer, Float> weights = new HashMap<Integer, Float>();
        int oaa = 1;
        int bits = 18;
        String line;
        boolean insideFeatures = false;
        while ((line = reader.readLine()) != null) {
            if (!insideFeatures) {
                if (line.startsWith("bits:")) {
                    bits = Integer.parseInt(line.split(":")[1]);
                } else if (line.startsWith("options:")) {
                    String[] parts = line.split(":");
                    if (parts.length > 1) {
                        String[] options = parts[1].split("\\s+");
                        for (int i = 0; i < options.length; i++) {
                            if ("--oaa".equals(options[i])) {
                                oaa = Integer.parseInt(options[i + 1]);
                                i++;
                            } else
                                logger.warn("Unhandled VW option - this model may not behave correctly at prediction time " + options[i]);
                        }
                    }
                } else if (line.startsWith(":0")) {
                    insideFeatures = true;
                }
            } else {
                String[] featureAndWeight = line.split(":");
                int feature = Integer.parseInt(featureAndWeight[0]);
                float weight = Float.parseFloat(featureAndWeight[1]);
                weights.put(feature, weight);
            }
        }
        return new VwModel(bits, oaa, weights, classIdMap);
    }

    @Override
    protected VwModel loadModel(String location, String client) {
        logger.info("Reloading VW model for client: " + client);

        try (
                BufferedReader modelReader = new BufferedReader(new InputStreamReader(
                        featuresFileHandler.getResourceStream(location + "/model")
                ))
        ) {
            Map<Integer, String> classIdMap;
            try (
                    BufferedReader modelReader2 = new BufferedReader(new InputStreamReader(
                            featuresFileHandler.getResourceStream(location + "/classes.txt")
                    ))
            ) {
                classIdMap = loadClassIdMap(modelReader2);
            } catch (IOException e) {
                logger.warn("Found no classes.txt for " + location);
                classIdMap = new HashMap<Integer, String>();
            }


            VwModel model = loadModel(modelReader, classIdMap);


            logger.info("Loaded VW model from " + location + " for " + client + " with " + model.weights.size() + " weights and " + model.classIdMap.size() + " classes");

            return model;
        } catch (FileNotFoundException e) {
            logger.error("Couldn't reload modelfor client " + client + " at " + location, e);
        } catch (IOException e) {
            logger.error("Couldn't reload model for client " + client + " at " + location, e);
        }
        return null;

    }

    public static class VwModel {
        public final int bits;
        public final int oaa;
        public final Map<Integer, String> classIdMap;
        public final Map<Integer, Float> weights;
        public final VwFeatureHash hasher;

        public VwModel(int bits, int oaa, Map<Integer, Float> weights, Map<Integer, String> classIdMap) {
            super();
            this.bits = bits;
            this.oaa = oaa;
            this.weights = weights;
            this.hasher = new VwFeatureHash(bits, oaa);
            this.classIdMap = classIdMap;
        }

        @Override
        public String toString() {
            return "VwModel [bits=" + bits + ", oaa=" + oaa + ", weights="
                    + weights + "]";
        }


    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/home/clive/work/seldon/external_prediction_server/vw/iris/model.txt"));
        VwModelManager m = new VwModelManager(null, null);
        VwModel vwModel = m.loadModel(br, new HashMap<Integer, String>());
        System.out.println(vwModel);
    }
}
