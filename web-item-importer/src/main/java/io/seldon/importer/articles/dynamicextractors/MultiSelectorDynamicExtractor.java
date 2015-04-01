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

import io.seldon.importer.articles.dynamicextractors.AttributeDetail.SubExtractor;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;


//@formatter:off
/*

example config:

{"attribute_detail_list":[
 {
     "name": "title",
     "is_required": true,
     "extractor_type": "MultiSelector",
     "extractor_args": [{"extr":"FirstElementTextValue","args":["[head > meta[property=og:title]"]}, {"extr":"FirstElementTextValue","args":["head > meta[property=og:tag]"]}],
     "default_value": ""
 }
]}

*/
//@formatter:on


public class MultiSelectorDynamicExtractor extends DynamicExtractor{

    @Override
    public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {
        
        
        for(SubExtractor subEx : attributeDetail.sub_types){
            // make a copy so as not to damage data.
            AttributeDetail detailCopy = copyAttrDetail(attributeDetail);
            
            detailCopy.extractor_args = subEx.extractor_args;
            String result = DynamicExtractor.build(subEx.extractor_type).extract(detailCopy, url, articleDoc);
            if(result!=null && !StringUtils.isBlank(result)){
                return result;
            }
        }
        return null;
    }

    public AttributeDetail copyAttrDetail(AttributeDetail attributeDetail) {
        AttributeDetail detailCopy = new AttributeDetail();
        detailCopy.default_value = attributeDetail.default_value;
        detailCopy.is_required = attributeDetail.is_required;
        detailCopy.name = attributeDetail.name;
        return detailCopy;
    }
    
}
