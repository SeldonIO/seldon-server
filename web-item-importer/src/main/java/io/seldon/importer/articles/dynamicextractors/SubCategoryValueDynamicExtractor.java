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

import io.seldon.importer.articles.category.CategoryExtractor;

import java.lang.reflect.Constructor;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;

//@formatter:off
/*

example config:

{"attribute_detail_list":[
    {
        "name": "subcategory",
        "is_required": false,
        "extractor_type": "SubCategoryValue",
        "extractor_args": ["GeneralAll"],
        "default_value": ""
    },
]}

*/
//@formatter:on

public class SubCategoryValueDynamicExtractor extends DynamicExtractor {

	@Override
	public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {

		String attrib_value = null;

		if ((attributeDetail.extractor_args != null) && (attributeDetail.extractor_args.size() == 1)) {
			String subCategoryClassPrefix = attributeDetail.extractor_args.get(0);

			if (StringUtils.isNotBlank(subCategoryClassPrefix)) {
				String className = "io.seldon.importer.articles.category."+subCategoryClassPrefix+"SubCategoryExtractor";
				Class<?> clazz = Class.forName(className);
				Constructor<?> ctor = clazz.getConstructor();
				CategoryExtractor extractor = (CategoryExtractor) ctor.newInstance();
				attrib_value = extractor.getCategory(url, articleDoc);
			}
		}

		return attrib_value;
	}

}
