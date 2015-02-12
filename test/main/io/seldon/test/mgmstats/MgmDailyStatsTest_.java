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

package io.seldon.test.mgmstats;

import java.io.IOException;
import java.sql.Timestamp;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.business.MgmDailyStatsBusinessService;
import org.junit.Test;

import io.seldon.api.resource.service.business.MgmDailyStatsBusinessServiceImpl;

public class MgmDailyStatsTest_ {

    @Test
    public void test_mgmDailyStatsBusinessServiceImpl() {

        MgmDailyStatsBusinessService mgmDailyStatsBusinessService = new MgmDailyStatsBusinessServiceImpl();

        final ConsumerBean consumerBean = new ConsumerBean("testclient");
        ResourceBean resourceBean = mgmDailyStatsBusinessService.getDailyStats(consumerBean);

        ObjectMapper om = new ObjectMapper();
        try {
            String json = om.writeValueAsString(resourceBean);
            System.out.println(json);

        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_mgmDailyStatsBusinessServiceImpl_timestampToString() {
        java.sql.Timestamp ts = new Timestamp(1387065600000L);
        System.out.println(ts);
        System.out.println(MgmDailyStatsBusinessServiceImpl.timestampToString(ts));
        
        java.sql.Timestamp ts2 = new Timestamp(System.currentTimeMillis());
        System.out.println(ts2);
        System.out.println(MgmDailyStatsBusinessServiceImpl.timestampToString(ts2));

    }
}
