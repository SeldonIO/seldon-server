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

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

// @formatter:off
/*

example config:

{"attribute_detail_list":[
    {
        "name": "tags",
        "is_required": false,
        "extractor_type": "FirstElementAttrListValue",
        "extractor_args": ["head > meta[name=keywords]", "content"],
        "default_value": ""
    },
]}

*/
// @formatter:on

public class FirstElementAttrListValueDynamicExtractor extends DynamicExtractor {

	@Override
	public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {

		String attrib_value = null;

		if ((attributeDetail.extractor_args != null) && (attributeDetail.extractor_args.size() >= 2)) {
			String cssSelector = attributeDetail.extractor_args.get(0);
			Element element = articleDoc.select(cssSelector).first();
			if (StringUtils.isNotBlank(cssSelector)) {
				int arg_count = 0;
				for (String value_name : attributeDetail.extractor_args) {
					if (arg_count > 0) { // skip the first one, its the cssSelector
						if (element != null && element.attr(value_name) != null) {
							String rawList = element.attr(value_name);
							if (StringUtils.isNotBlank(rawList)) {
								String[] parts = rawList.split(",");
								for (int i = 0; i < parts.length; i++) {
									parts[i] = parts[i].trim().toLowerCase();
								}
								attrib_value = StringUtils.join(parts, ',');
								break;
							}
						}
					}
					arg_count++;
				}
			}
		}

		return attrib_value;
	}

}
