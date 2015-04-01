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

package io.seldon.api;

import java.util.Date;
import java.util.Properties;

public class TestingUtils {

	private static TestingUtils tutil = new TestingUtils();
	
	public static void initialise(Properties props)
	{
		tutil = new TestingUtils(props);
	}
	
	public static TestingUtils get()
	{
		return tutil;
	}
	
	private TestingUtils()
	{
		testing = false;
	}
	
	private TestingUtils(Properties props)
	{
		String testStr = props.getProperty("io.seldon.testing");
		if (testStr != null && "true".equals(testStr))
			testing = true;
		else
			testing = false;
	}
	
	private boolean testing = false;
	private Date lastActionTime = null;
		
	public void setTesting(boolean val)
	{
		testing = val;
	}
	
	public boolean getTesting()
	{
		return testing;
	}

	public Date getLastActionTime() {
		return lastActionTime;
	}

	public void setLastActionTime(Date lastActionTime) {
		this.lastActionTime = lastActionTime;
	}

	/**
	 * Returns a time based on last action time if in testing mode or current time. Returns number of seconds since epoch.
	 * @return
	 */
	public static long getTime()
	{
		if (get() != null && get().getTesting() && get().getLastActionTime() != null)
			return get().getLastActionTime().getTime()/1000;
		else
			return System.currentTimeMillis()/1000;
	}
	
	
	
	
}
