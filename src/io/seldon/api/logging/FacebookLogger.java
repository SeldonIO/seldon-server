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

import org.apache.log4j.Logger;

public class FacebookLogger {
	
	private static Logger apiLogger = Logger.getLogger( FacebookLogger.class );

	public static enum Result { SUCCESS, ERROR_OAUTH, ERROR_GRAPH, ERROR_FQL, ERROR_NETWORK, ERROR }
	
	public static void log(Date start, Date end, String fbId, int likes, Result result) {
		String event = String.format("%s,%s,%s,%d",result.toString(),fbId,likes,end.getTime() - start.getTime());
		apiLogger.info(event);
	}
	
}
