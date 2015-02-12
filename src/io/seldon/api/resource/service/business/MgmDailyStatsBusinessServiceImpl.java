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

package io.seldon.api.resource.service.business;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.MgmDailyStatBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.db.jdo.JDOFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.seldon.api.resource.MgmDailyStatsBean;

@Component
public class MgmDailyStatsBusinessServiceImpl implements MgmDailyStatsBusinessService {

    private static final Logger logger = LoggerFactory.getLogger(MgmDailyStatsBusinessService.class);

    private static interface MgmDailyStatsDataReceiver {
        public boolean processMgmDailyStatsData(java.sql.Timestamp day, Integer impressions, Integer shares, Integer fbclicks, Integer conversions);
    }

    @Override
    public ResourceBean getDailyStats(ConsumerBean consumerBean) throws APIException {

        String client = consumerBean.getShort_name();

        final MgmDailyStatsBean resourceBean = new MgmDailyStatsBean(client);
        doDailyStatsQuery(client, new MgmDailyStatsDataReceiver() {

            @Override
            public boolean processMgmDailyStatsData(java.sql.Timestamp day, Integer impressions, Integer shares, Integer fbclicks, Integer conversions) {
                String day_string = timestampToString(day);
                resourceBean.addMgmDailyStatBean(new MgmDailyStatBean(day_string, impressions, shares, fbclicks, conversions));
                return false;
            }
        });

        return resourceBean;
    }

    public static String timestampToString(java.sql.Timestamp ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
    
    private static void doDailyStatsQuery(String client, MgmDailyStatsDataReceiver mgmDailyStatsDataReceiver) throws APIException {

        try {
            PersistenceManager pm = JDOFactory.getPersistenceManager("mgmstats");
            String sql = "select day,impressions,shares,fbclicks,conversions from daily d where d.client=?;";
            Query query = pm.newQuery("javax.jdo.query.SQL", sql);
            ArrayList<Object> args = new ArrayList<Object>();
            args.add(client);
            Collection<Object[]> results = (Collection<Object[]>) query.executeWithArray(args.toArray());
            for (Object[] item : results) {
                // Matching the query columns
                final java.sql.Timestamp day = (java.sql.Timestamp) item[0];
                final Integer impressions = (Integer) item[1];
                final Integer shares = (Integer) item[2];
                final Integer fbclicks = (Integer) item[3];
                final Integer conversions = (Integer) item[4];
                if (mgmDailyStatsDataReceiver != null) {
                    mgmDailyStatsDataReceiver.processMgmDailyStatsData(day, impressions, shares, fbclicks, conversions);
                }
            }
            query.closeAll();
        } catch (Exception e) {
            logger.error("Database error during daily stats query", e);
			throw new APIException(APIException.INTERNAL_DB_ERROR);
        }
    }
}
