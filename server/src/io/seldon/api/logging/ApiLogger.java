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

import java.util.Date;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.statsd.StatsdPeer;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import org.apache.log4j.Logger;

/**
 * Core API logger for output to provide data for batch/streaming analysis
 * @author rummble
 *
 */
public class ApiLogger {

	private static Logger apiLogger = Logger.getLogger( "ApiStatsLogger" );

	public static void log(String apiKey,Date start,Date end, ResourceBean con, ResourceBean res, HttpServletRequest req)
	{
		log(apiKey,start,end,  con,  res,  req, null);
	}
	
	public static void log(String apiKey,Date start,Date end, ResourceBean con, ResourceBean res, HttpServletRequest req, String algorithm) {
		String consumerName = "undefined";
		long errorCode = 0;
		if(con instanceof ConsumerBean) { consumerName = ((ConsumerBean)con).getShort_name(); }
		if(res instanceof ErrorBean) { errorCode = ((ErrorBean)res).getError_id(); }
		String bean = res.toLog();
		String event = " ";
		String query = req.getQueryString();
		if (query != null)
			query = query.replaceAll(",","%2C");
		if(bean == null) {
			event = String.format("%d,%s,%s,%s,%s,%s,%d,%s",errorCode,consumerName,req.getMethod(),req.getServerName() + req.getContextPath(),req.getServletPath(),query,end.getTime() - start.getTime(),UUID.randomUUID());
		}
		else if(algorithm == null) {
			event = String.format("%d,%s,%s,%s,%s,%s,%d,%s,%s",errorCode,consumerName,req.getMethod(),req.getServerName() + req.getContextPath(),req.getServletPath(),query,end.getTime() - start.getTime(),UUID.randomUUID(),bean);
		}
		else {
			event = String.format("%d,%s,%s,%s,%s,%s,%d,%s,%s,%s",errorCode,consumerName,req.getMethod(),req.getServerName() + req.getContextPath(),req.getServletPath(),query,end.getTime() - start.getTime(),UUID.randomUUID(),bean,algorithm);
		}
		apiLogger.info(event);
		StatsdPeer.logAPICall(consumerName, apiKey, req.getMethod().toLowerCase(),(int)(end.getTime() - start.getTime()));
	}

    public static void log(String apiKey,Date start, HttpServletRequest request, ConsumerBean consumerBean) {
        log(apiKey,start, new Date(), consumerBean, new ListBean(), request);
    }

}
