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

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//@formatter:off
/*

example config:

{"attribute_detail_list":[
   {
       "name": "title",
       "is_required": true,
       "extractor_type": "FirstElementTextValue",
       "extractor_args": ["h1"],
       "default_value": ""
   }
]}

*/
//@formatter:on

public class FirstElementTextValueDynamicExtractor extends DynamicExtractor {
	static Pattern pattern = null;
	
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
		
		if ((attrib_value!=null) && (attributeDetail.extractor_args != null) && (attributeDetail.extractor_args.size() >= 2)) {	
			String regexSelector = attributeDetail.extractor_args.get(1);
			pattern = Pattern.compile(regexSelector);
			Matcher m = pattern.matcher(attrib_value);
			m.find();
			attrib_value = m.group(1);
		}
		
		return attrib_value;
	}
		

}
