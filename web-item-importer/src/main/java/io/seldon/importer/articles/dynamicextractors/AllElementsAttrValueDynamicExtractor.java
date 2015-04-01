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
import org.jsoup.select.Elements;

//@formatter:off
/*

example config:

{"attribute_detail_list":[
 {
     "name": "tags",
     "is_required": false,
     "extractor_type": "AllElementsAttrValue",
     "extractor_args": ["section[id=tags] > a", "title],
     "default_value": ""
 }
]}

*/
//@formatter:on
public class AllElementsAttrValueDynamicExtractor extends DynamicExtractor {

    @Override
    public String extract(AttributeDetail attributeDetail, String url,
            Document articleDoc) throws Exception {
        String attrib_value = null;

        if ((attributeDetail.extractor_args != null) && (attributeDetail.extractor_args.size() >= 2)) {
            String cssSelector = attributeDetail.extractor_args.get(0);
            String attributeName = attributeDetail.extractor_args.get(1);
            Elements elements = articleDoc.select(cssSelector);
            if (StringUtils.isNotBlank(cssSelector)) {
                if (elements != null) {
                    StringBuilder sb = new StringBuilder();
                    boolean isFirstInList=true;
                    for (Element e : elements) {
                        String eText = e.attr(attributeName);
                        eText = StringUtils.strip(eText);
                        eText = eText.toLowerCase();
                        if(StringUtils.isBlank(eText)) continue;
                        if (isFirstInList) {
                            isFirstInList = false;
                        } else {
                            sb.append(",");
                        }
                        sb.append(eText);
                    }
                    attrib_value = sb.toString();
                }
            }
        }

        return attrib_value;
    }

}
