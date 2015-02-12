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

package io.seldon.facebook.user.algorithm.experiment;

import io.seldon.general.MgmAction;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author philipince
 *         Date: 28/10/2013
 *         Time: 14:23
 */
public class MultiVariateTestResult {

    private final Date startDate;
    private final Date outputDate;
    private final String clientKey;
    private final String variationLabel;
    private final Map<MgmAction.MgmActionType, AtomicInteger> actions;
    private final BigDecimal testRangeSize;

    public MultiVariateTestResult(Date startDate, Date outputDate, String clientKey, String variationLabel,
                                  Map<MgmAction.MgmActionType, AtomicInteger> actions, BigDecimal testRangeSize) {
        this.startDate = startDate;
        this.outputDate = outputDate;
        this.clientKey = clientKey;
        this.variationLabel = variationLabel;
        this.actions = actions;
        this.testRangeSize = testRangeSize;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getOutputDate() {
        return outputDate;
    }

    public String getClientKey() {
        return clientKey;
    }

    public String getVariationLabel() {
        return variationLabel;
    }


    public BigDecimal getTestRangeSize() {
        return testRangeSize;
    }

    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append("Variation "+variationLabel + " of size " + testRangeSize + '\n');
        AtomicInteger imps = actions.get(MgmAction.MgmActionType.IMPRESSION);
        AtomicInteger newShares = actions.get(MgmAction.MgmActionType.NEW_USR_SHARE);
        AtomicInteger oldShares = actions.get(MgmAction.MgmActionType.EXISTING_USR_SHARE);
        AtomicInteger clicks = actions.get(MgmAction.MgmActionType.FB_REC_CLICK);
        AtomicInteger newConversions = actions.get(MgmAction.MgmActionType.NEW_USR_CONVERSION);
        AtomicInteger oldConversions = actions.get(MgmAction.MgmActionType.EXISTING_USR_CONVERSION);
        builder.append('\t' + " Impressions : "+ imps+ '\n');
        builder.append('\t' + " New user shares : "+ newShares + '\n');
        builder.append('\t' + " Existing user shares : "+ oldShares + '\n');
        builder.append('\t' + " Clicks : "+ clicks+ '\n');
        builder.append('\t' + " New user conversions : "+ newConversions+ '\n');
        builder.append('\t' + " Existing user conversions : "+ oldConversions+ '\n');
        return builder.toString();
    }

}
