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

package io.seldon.facebook.client;

import java.util.List;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.Parameter;
import org.apache.log4j.Logger;

/**
 * Each of the overridden methods in this class are throttled.
 *
 * Created by: marc on 16/08/2011 at 14:19
 */
public class ThrottledFacebookClient extends DefaultFacebookClient {

    private static final Logger logger = Logger.getLogger(ThrottledFacebookClient.class);

    private final static int THROTTLE_INTERVAL = 1500;

    // private final static int FREE_RIDE_COUNT = 500;
    private final static int FREE_RIDE_COUNT = 0; // for testing

    private int completedCalls = 0;

    public ThrottledFacebookClient(String oauthString) {
        super(oauthString);
    }

    @Override
    public <T> Connection<T> fetchConnection(String connection, Class<T> connectionType, Parameter... parameters) {
        long start = System.currentTimeMillis();
        Connection<T> ret = super.fetchConnection(connection, connectionType, parameters);
        throttle(start);
        return ret;
    }

    @Override
    public <T> Connection<T> fetchConnectionPage(String connectionPageUrl, Class<T> connectionType) {
        long start = System.currentTimeMillis();
        Connection<T> ret = super.fetchConnectionPage(connectionPageUrl, connectionType);
        throttle(start);
        return ret;
    }

    @Override
    public <T> T fetchObject(String object, Class<T> objectType, Parameter... parameters) {
        long start = System.currentTimeMillis();
        T ret = super.fetchObject(object, objectType, parameters);
        throttle(start);
        return ret;
    }

    @Override
    public <T> T fetchObjects(List<String> ids, Class<T> objectType, Parameter... parameters) {
        long start = System.currentTimeMillis();
        T ret = super.fetchObjects(ids, objectType, parameters);
        throttle(start);
        return ret;
    }

    private void throttle(long start) {
        if ( completedCalls++ < FREE_RIDE_COUNT) {
            logger.info("Within the threshold, not sleeping.");
            return;
        }
        long finish = System.currentTimeMillis();
        long elapsed = finish - start;
        if (elapsed < THROTTLE_INTERVAL) {
            try {
                long remaining = THROTTLE_INTERVAL - elapsed;
                logger.info("Sleeping for: " + remaining + "ms; call took: " + elapsed + "ms");
                Thread.sleep(remaining);
            } catch (InterruptedException e) {
                logger.error("Throttled sleep interrupted.", e);
            }
        } else {
            logger.info("Call took: " + elapsed + "ms; exceeds minimum call time by "
                    + (elapsed - THROTTLE_INTERVAL) + "ms, no need to sleep.");
        }
    }

}
