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

import java.util.ArrayList;
import java.util.List;


public class MgmDailyStatsBean extends ResourceBean {

    private static final long serialVersionUID = 1L;

    final private List<MgmDailyStatBean> days = new ArrayList<>();
    final private String client;

    public MgmDailyStatsBean(String client) {
        this.client = client;
    }

    public void addMgmDailyStatBean(MgmDailyStatBean mgmDailyStatBean) {
        days.add(mgmDailyStatBean);
    }

    @Override
    public String toKey() {
        return null;
    }

    @JsonProperty("client")
    public String getClient() {
        return client;
    }

    @JsonProperty("days")
    public List<MgmDailyStatBean> getDays() {
        return days;
    }
}
