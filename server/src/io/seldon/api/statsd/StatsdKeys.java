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

package io.seldon.api.statsd;

public class StatsdKeys {


	public static String getAPIKey(String client,String apiKey,String method)
	{
		return "api."+StatsdPeer.installId+"."+client+"."+apiKey+"."+method;
	}
	
	public static String getClick(String client,String algKey)
	{
		return "api."+StatsdPeer.installId+"."+client+".click."+algKey.toLowerCase()+".total";
	}

	public static String getClickABTesting(String client,String algKey,String abTestingKey)
	{
		return "api."+StatsdPeer.installId+"."+client+".click."+abTestingKey+"."+algKey.toLowerCase()+".total";
	}
	
	public static String getClickABTesting(String client,String algKey,String abTestingKey,String recTag)
	{
		return "api."+StatsdPeer.installId+"."+client+".click."+recTag+"."+abTestingKey+"."+algKey.toLowerCase()+".total";
	}

	
	public static String getPositiveClick(String client,String algKey)
	{
		return "api."+StatsdPeer.installId+"."+client+".click."+algKey.toLowerCase()+".positive";
	}
	
	public static String getPositiveClickABTesting(String client,String algKey,String abTestingKey)
	{
		return "api."+StatsdPeer.installId+"."+client+".click."+abTestingKey+"."+algKey.toLowerCase()+".positive";
	}

	public static String getPositiveClickABTesting(String client,String algKey,String abTestingKey,String recTag)
	{
		return "api."+StatsdPeer.installId+"."+client+".click."+recTag+"."+abTestingKey+"."+algKey.toLowerCase()+".positive";
	}

	
	public static String getImpression(String client) {
		return "api."+StatsdPeer.installId+"."+client+".clicks.total";
	}

	public static String getImpression(String client,String recTag) {
		return "api."+StatsdPeer.installId+"."+client+"."+recTag+".clicks.total";
	}
	
	public static String getClick(String client) {
		return "api."+StatsdPeer.installId+"."+client+".clicks.positive";
	}

	public static String getClickWithRecTag(String client,String recTag) {
		return "api."+StatsdPeer.installId+"."+client+"."+recTag+".clicks.positive";
	}


}
