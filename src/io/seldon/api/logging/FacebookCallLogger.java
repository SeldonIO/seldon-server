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

/**
 * Created by dylanlentini on 31/01/2014.
 */


public class FacebookCallLogger implements FacebookCallListener
{

    int fbCall;
    private static Logger facebookCallLogger = Logger.getLogger("FacebookCallLogger");

    public FacebookCallLogger()
    {
        fbCall = 0;
    }

    @Override
    public void fbCallPerformed()
    {
        fbCall++;
    }

    public int getFbCalls()
    {
        return fbCall;
    }


    @Override
    public void log(String fbAlgorithm, String consumerName, String fbUser, String fbToken)
    {
        String event =
                    String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                    fbAlgorithm,
                    consumerName,
                    fbUser,
                    fbToken,
                    fbCall);

        facebookCallLogger.info(event);
        //StatsdPeer.logFbCallsStat(fbAlgorithm, consumerName, fbUser, fbToken, numberFbCalls);
    }


}


