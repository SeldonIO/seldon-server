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

package io.seldon.recommendation.userdimensionmapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

@Component
public class UserDimensionMappingModelManager extends ModelManager<UserDimensionMappingModelManager.UserDimensionMappingModel> {

    private static Logger logger = Logger.getLogger(UserDimensionMappingModelManager.class.getName());
    private final ExternalResourceStreamer dimensionsFileHandler;
    public static final String USER_DIM_MODEL_LOC_PATTERN = "user_dim_model";
    private final Map<String, UserDimensionMappingModel> client_userDimensionMappingModel = new HashMap<>();

    @Autowired
    public UserDimensionMappingModelManager(ExternalResourceStreamer dimensionsFileHandler, NewResourceNotifier notifier) {
        super(notifier, Collections.singleton(USER_DIM_MODEL_LOC_PATTERN));
        this.dimensionsFileHandler = dimensionsFileHandler;
        logger.info("Initializing");
    }

    @Override
    protected UserDimensionMappingModel loadModel(String location, String client) {
        logger.info("Reloading user dimensions for client[" + client + "]");

        UserDimensionMappingModel model = null;
        try {
            // location = location + "/part-00000";
            BufferedReader reader = new BufferedReader(new InputStreamReader(dimensionsFileHandler.getResourceStream(location)));
            model = loadUserDimensionMapping(reader);
            reader.close();

            int num_users = (model != null) ? model.userToDimensions.size() : 0;
            logger.info("Loaded user dimensions for client[" + client + "] users[" + num_users + "]");

        } catch (IOException e) {
            logger.error("Couldn't reload user dimensions for client " + client, e);
        } catch (Exception e) {
            logger.error("Couldn't reload user dimensions for client " + client, e);
        }

        if (model != null) {
            client_userDimensionMappingModel.put(client, model);
        } else {
            client_userDimensionMappingModel.remove(client);
        }

        return model;
    }

    public UserDimensionMappingModel loadUserDimensionMapping(BufferedReader reader) throws IOException {

        Map<Long, DimensionMapping> userToDimensions = new HashMap<Long, DimensionMapping>();
        String line;
        ObjectMapper mapper = new ObjectMapper();
        while ((line = reader.readLine()) != null) {
            DimensionMapping userDimensionMapping = mapper.readValue(line, DimensionMapping.class);
            userToDimensions.put(userDimensionMapping.userid, userDimensionMapping);
        }
        UserDimensionMappingModel model = new UserDimensionMappingModel(userToDimensions);

        return model;
    }

    public Set<Integer> getMappedDimensionsByUser(String client, Set<Integer> dimensions, long userid) {
        UserDimensionMappingModel userDimensionMappingModel = client_userDimensionMappingModel.get(client);
        if (userDimensionMappingModel == null) {
            logger.info(">> No mappings for client");
            return dimensions; // no mappings for this client so return input
        }

        DimensionMapping dimensionMapping = userDimensionMappingModel.userToDimensions.get(userid);
        if (dimensionMapping == null) {
            logger.info(">> No mappings for user");
            return dimensions; // no mappings for this userid so return input
        }

        Set<Integer> mapped_dimensions = new HashSet<Integer>();

        for (Integer dimension : dimensions) {
            if (dimensionMapping.dims_in.contains(dimension)) {
                mapped_dimensions.addAll(dimensionMapping.dims_out);
            } else {
                mapped_dimensions.add(dimension);
            }
        }

        return mapped_dimensions;
    }

    public static class DimensionMapping {
        long userid;
        Set<Integer> dims_in;
        Set<Integer> dims_out;

        public DimensionMapping() {
        }

        public long getUserid() {
            return userid;
        }

        public void setUserid(long userid) {
            this.userid = userid;
        }

        public Set<Integer> getDims_in() {
            return dims_in;
        }

        public void setDims_in(Set<Integer> dims_in) {
            this.dims_in = dims_in;
        }

        public Set<Integer> getDims_out() {
            return dims_out;
        }

        public void setDims_out(Set<Integer> dims_out) {
            this.dims_out = dims_out;
        }

        @Override
        public String toString() {
            String output = String.format("{userid:%d, dims_in:%s, dims_out:%s}", userid, dims_in, dims_out);
            return output;
        }
    }

    public static class UserDimensionMappingModel {
        final Map<Long, DimensionMapping> userToDimensions;

        public UserDimensionMappingModel(Map<Long, DimensionMapping> userToDimensions) {
            this.userToDimensions = userToDimensions;
        }
    }
}
