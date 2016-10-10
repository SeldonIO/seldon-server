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
package io.seldon.api.state;

import io.seldon.api.state.PredictionAlgorithmStore.Strategy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class PredictionStrategyDeserializer extends StdDeserializer<Strategy> {
    private Map<String, Class<? extends Strategy>> registry = new HashMap<>();
private static Logger logger = Logger.getLogger(PredictionStrategyDeserializer.class.getName());
    public PredictionStrategyDeserializer() {
        super(Strategy.class);
        registerStrategyType("variations", PredictionAlgorithmStore.TestConfig.class);
        registerStrategyType("algorithms",PredictionAlgorithmStore.AlgorithmConfig.class);
    }
    void registerStrategyType(String uniqueAttribute
            ,Class<? extends Strategy> strategyClass) {
        registry.put(uniqueAttribute, strategyClass);
    }
    @Override
    public Strategy deserialize(
            JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();
        ObjectNode root = mapper.readTree(jp);
        Class<? extends Strategy> theClass = null;
        Iterator<Map.Entry<String, JsonNode>> elementsIterator =
                root.fields();
        while (elementsIterator.hasNext()) {
            Map.Entry<String, JsonNode> element = elementsIterator.next();
            String name = element.getKey();
            Class<? extends Strategy> possibility = registry.get(name);
            if(possibility!=null){
                theClass = possibility;
//                break;
            }
        }
        if (registry == null) {
            return null;
        }
        return mapper.treeToValue(root, theClass);
    }

}

