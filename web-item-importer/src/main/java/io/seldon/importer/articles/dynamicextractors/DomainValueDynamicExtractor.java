/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.importer.articles.dynamicextractors;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

//@formatter:off
/*

example config:

{"attribute_detail_list":[
    {
        "name": "domain",
        "is_required": false,
        "extractor_type": "DomainValue",
        "extractor_args": [],
        "default_value": ""
    }
]}

*/
//@formatter:off

public class DomainValueDynamicExtractor extends DynamicExtractor {

	protected static final String UNKOWN_DOMAN = "UNKOWN_DOMAN";
    private static Logger logger = Logger.getLogger(DomainValueDynamicExtractor.class.getName());
	
	@Override
	public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {

		String attrib_value = getDomain(url);
		
		return attrib_value;
	}

	/**
	 * 
	 * @param url The domain to extract the domain from.
	 * @return The domain or UNKOWN_DOMAN if unable to use url.
	 */
	private static String getDomain(String url) {
		String retVal = UNKOWN_DOMAN;
		if (!url.startsWith("http") && !url.startsWith("https")) {
			url = "http://" + url;
		}
		URL netUrl = null;
		try {
			netUrl = new URL(url);
		} catch (MalformedURLException e) {
			logger.warn("Failed to get domain for "+ url);
		}
		if (netUrl != null) {
			String host = netUrl.getHost();
			retVal = host;
		}

		return retVal;
	}

}
