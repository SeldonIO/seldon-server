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
        "extractor_type": "FirstElementAttrValueDateWithFormat",
        "extractor_args": ["meta[name=Created-date]", "yyyy-MM-dd'T'HH:mm:ss", "content"],
        "default_value": ""
    }   
]}

*/
//@formatter:on

public class FirstElementAttrValueDateWithFormatDynamicExtractor extends DynamicExtractor {

	private static Logger logger = Logger.getLogger(FirstElementAttrValueDateWithFormatDynamicExtractor.class.getName());

	@Override
	public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {

		String attrib_value = null;
		String dateFormatString = null;
		
		if ((attributeDetail.extractor_args != null) && (attributeDetail.extractor_args.size() >= 3)) {
			String cssSelector = attributeDetail.extractor_args.get(0);
			dateFormatString = attributeDetail.extractor_args.get(1);
			Element element = articleDoc.select(cssSelector).first();
			if (StringUtils.isNotBlank(cssSelector)) {
				int arg_count = 0;
				for (String value_name : attributeDetail.extractor_args) {
					if (arg_count > 1) { // skip the first one, its the cssSelector, and second thats the Date format
						if (element != null && element.attr(value_name) != null) {
							attrib_value = element.attr(value_name);
							if (StringUtils.isNotBlank(attrib_value)) {
								break;
							}
						}
					}
					arg_count++;
				}
			}
		}

		if ((attrib_value != null) && (dateFormatString != null)) {
			String pubtext = attrib_value;
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			DateFormat df = new SimpleDateFormat(dateFormatString, Locale.ENGLISH);
			Date result = null;
			try {
				result = df.parse(pubtext);
			} catch (ParseException e) {
				logger.info("Failed to parse date with format ["+dateFormatString+"] " + pubtext);
			}

			if (result != null) {
			    String attrib_value_orig = attrib_value;
				attrib_value = dateFormatter.format(result);
				String msg = "Extracted date ["+attrib_value_orig+"] - > ["+attrib_value+"]";
                logger.info(msg);
			} else {
				logger.error("Failed to parse date " + pubtext);
				attrib_value = null;
			}
		}

		return attrib_value;
	}

}
