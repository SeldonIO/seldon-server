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

import org.jsoup.nodes.Document;

public class UrlSectionDynamicExtractor extends DynamicExtractor {

    @Override
    public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {
        String urlWithoutProtocol = url.replace("http://", "");
        String[] urlSplit = urlWithoutProtocol.split("/");
        if(attributeDetail.extractor_args.isEmpty()) return null;
        int sectionNumber = Integer.parseInt(attributeDetail.extractor_args.get(0));
        if(!(urlSplit.length > (sectionNumber+1))) return null;
        return urlSplit[sectionNumber];
    }

}
