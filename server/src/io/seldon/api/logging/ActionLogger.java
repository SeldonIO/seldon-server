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

public class ActionLogger {
	private static Logger actionLogger = Logger.getLogger( "ActionLogger" );
	
	public static void log(String client,long userId,long itemId,Integer type,Double value,String clientUserId,String clientItemId,String recTag)
	{
		if (type == null) { type = 1; }
		if (value == null) { value = 1D; }
		String event = String.format("%s,%s,%s,%s,%s,%s,\"%s\",\"%s\"",client,recTag != null ? recTag : "default",userId,itemId,type,value,clientUserId,clientItemId);
		actionLogger.info(event);
	}
}
