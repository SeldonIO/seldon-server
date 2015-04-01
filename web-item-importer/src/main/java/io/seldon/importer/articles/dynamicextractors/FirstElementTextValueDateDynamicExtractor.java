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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

//@formatter:off
/*

example config:

{"attribute_detail_list":[
    {
        "name": "published_date",
        "is_required": false,
        "extractor_type": "FirstElementTextValueDate",
        "extractor_args": ["h1"],
        "default_value": ""
    }   
]}

*/
//@formatter:on

public class FirstElementTextValueDateDynamicExtractor extends DynamicExtractor {

	private static Logger logger = Logger.getLogger(FirstElementAttrValueDateDynamicExtractor.class.getName());

	@Override
	public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {

		String attrib_value = null;

		if ((attributeDetail.extractor_args != null) && (attributeDetail.extractor_args.size() >= 1)) {
			String cssSelector = attributeDetail.extractor_args.get(0);
			Element element = articleDoc.select(cssSelector).first();
			if (StringUtils.isNotBlank(cssSelector)) {
				if (element != null) {
					attrib_value = element.text();
				}
			}
		}

		if (attrib_value != null) {
			String pubtext = attrib_value;
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			DateFormat df = new SimpleDateFormat("dd/mm/yyyy hh:mm", Locale.ENGLISH);
			Date result = null;
			try {
				result = df.parse(pubtext);
			} catch (ParseException e) {
				logger.info("Failed to parse date withUTC format " + pubtext);
			}
			// try a simpler format
			df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
			try {
				result = df.parse(pubtext);
			} catch (ParseException e) {
				logger.info("Failed to parse date " + pubtext);
			}			

			if (result != null) {
				attrib_value = dateFormatter.format(result);
			} else {
				logger.error("Failed to parse date " + pubtext);
			}
					
		}

		return attrib_value;
	}

}

