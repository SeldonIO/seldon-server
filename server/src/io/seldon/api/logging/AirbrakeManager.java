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

package io.seldon.api.logging;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import airbrake.AirbrakeAppender;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.seldon.api.state.GlobalConfigHandler;
import io.seldon.api.state.GlobalConfigUpdateListener;

@Component
public class AirbrakeManager implements GlobalConfigUpdateListener {

    public static class AirbrakeConfig {
        public String api_key;
        public String env;
        public boolean enabled = false;

        @Override
        public String toString() {
            return String.format("%s[api_key[%s], env[%s], enabled[%s]]", this.getClass().getSimpleName(), api_key, env, enabled);
        }
    }

    private static Logger logger = Logger.getLogger(AirbrakeManager.class.getName());
    private final String ZK_CONFIG_KEY_AIRBRAKE = "airbrake";
    private final String ZK_CONFIG_KEY_AIRBRAKE_FPATH = "/config/" + ZK_CONFIG_KEY_AIRBRAKE;

    @Autowired
    public AirbrakeManager(GlobalConfigHandler globalConfigHandler) {
        logger.info("Subscribing for updates to " + ZK_CONFIG_KEY_AIRBRAKE_FPATH);
        globalConfigHandler.addSubscriber(ZK_CONFIG_KEY_AIRBRAKE, this);
    }

    @Override
    public void configUpdated(String configKey, String configValue) {
        logger.info(String.format("received config update %s[%s]", configKey, configValue));
        if (configValue.length() > 0) {
            AirbrakeConfig airbrakeConfig = buildAirbrakeConfigFromJson(configValue);
            createAirbrakeAppender(airbrakeConfig);
        }
    }

    private static AirbrakeConfig buildAirbrakeConfigFromJson(String json) {
        AirbrakeConfig retVal = null;

        ObjectMapper mapper = new ObjectMapper();
        try {
            retVal = mapper.readValue(json, AirbrakeConfig.class);
        } catch (Exception e) {
            throw new RuntimeException(String.format("* Error * building AirbrakeConfig from json[%s]", json), e);
        }

        return retVal;
    }

    private static void createAirbrakeAppender(AirbrakeConfig airbrakeConfig) {
        logger.info("creating AirbrakeAppender using: " + airbrakeConfig);
        AirbrakeAppender airbrakeAppender = new AirbrakeAppender();
        airbrakeAppender.setApi_key(airbrakeConfig.api_key);
        airbrakeAppender.setEnv(airbrakeConfig.env);
        airbrakeAppender.setEnabled(airbrakeConfig.enabled);
        Logger.getRootLogger().addAppender(airbrakeAppender);
    }
}
