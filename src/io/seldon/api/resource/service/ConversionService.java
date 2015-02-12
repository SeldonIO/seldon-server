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

package io.seldon.api.resource.service;

import io.seldon.api.logging.MgmLogger;
import io.seldon.api.resource.ConversionBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.general.MgmAction;

/**
 * @author philipince
 *         Date: 31/01/2014
 *         Time: 11:54
 */
public abstract class ConversionService {


    final MultiVariateTestStore mvTestStore;

    protected ConversionService(MultiVariateTestStore mvTestStore) {
        this.mvTestStore = mvTestStore;
    }


    public ResourceBean registerConversion(String consumer, ConversionBean conversionBean) {
        MgmAction action = new MgmAction(conversionBean.getInviter(), conversionBean.getInvitee(), conversionBean.getDate(),
                conversionBean.getType()== ConversionBean.ConversionType.EXISTING_USER?
                        MgmAction.MgmActionType.EXISTING_USR_CONVERSION:
                        MgmAction.MgmActionType.NEW_USR_CONVERSION, null, null);
        String testVariationKey= null;
        if(mvTestStore.testRunning(consumer)){
            testVariationKey = mvTestStore.retrieveVariationKey(consumer, conversionBean.getInviter());
            mvTestStore.registerTestEvent(consumer, action);
        }
        MgmLogger.log(consumer, action, testVariationKey);
        return conversionBean;
    }
}
