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

package io.seldon.api.resource;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MgmDailyStatBean extends ResourceBean {

    private static final long serialVersionUID = 1L;

    final private String day; // formatted as: '2013-12-15 00:00:00'
    final private int impressions;
    final private int shares;
    final private int fbclicks;
    final private int conversions;

    public MgmDailyStatBean(String day, int impressions, int shares, int fbclicks, int conversions) {
        this.day = day;
        this.impressions = impressions;
        this.shares = shares;
        this.fbclicks = fbclicks;
        this.conversions = conversions;
    }

    @Override
    public String toKey() {
        return null;
    }

    @JsonProperty("day")
    public String getDay() {
        return day;
    }

    @JsonProperty("impressions")
    public int getImpressions() {
        return impressions;
    }

    @JsonProperty("shares")
    public int getShares() {
        return shares;
    }

    @JsonProperty("fbclicks")
    public int getFbClicks() {
        return fbclicks;
    }

    @JsonProperty("conversions")
    public int getConversions() {
        return conversions;
    }

}
