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

/**
 * Extracts a part of the domain for hostnames with sub domains.
 * @author philipince
 *
 */
public class SubDomainDynamicExtractor extends DomainValueDynamicExtractor {

    public String extract(AttributeDetail attributeDetail, String url, Document articleDoc) throws Exception {
        String domain = super.extract(attributeDetail, url, articleDoc);
        if(domain.equals(UNKOWN_DOMAN)){
            return null;
        } else {
            // to parse a host, we must remove the suffix (this is a very poor way of doing it)
            String[] suffixList = {".com", ".co.uk", ".cat", ".it"};
            String domainWithoutSuffix = null;
            for(String suffix : suffixList){
                if(domain.contains(suffix)){
                    domainWithoutSuffix = domain.replaceFirst(suffix, "");
                    break;
                }                
            }
            String[] prefixList = {"www."};
            for(String prefix : prefixList){
                if(domain.contains(prefix)){
                    domainWithoutSuffix = domainWithoutSuffix.replaceFirst(prefix, "");
                    break;
                }                
            }
            if(domainWithoutSuffix == null || domainWithoutSuffix.isEmpty()) return null;
            String[] domainSplit = domainWithoutSuffix.split("\\.");
            // only look at domains like subdomain.domain
            int subdomainPosition = 1;
            if(!attributeDetail.extractor_args.isEmpty())
                subdomainPosition = Integer.parseInt(attributeDetail.extractor_args.get(0));
            
            if(domainSplit.length <= subdomainPosition) return null;
            
            return domainSplit[subdomainPosition-1];
             
        }
    }
    
}
