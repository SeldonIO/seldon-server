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

import java.util.ArrayList;
import java.util.List;

public class AttributeDetail {
    public String name = ""; //eg. "title" or "image"
    public boolean is_required = false; // true or false
    public String extractor_type = ""; //eg. CSS_SELECTOR|CLASS_PREFIX|CSS_SELECTOR_FOR_DATE|CSS_SELECTOR_FOR_NER
    public List<String> extractor_args = new ArrayList<String>(); //eg. ["head > meta[property=og:title]", "content"]
    public List<SubExtractor> sub_types = new ArrayList<AttributeDetail.SubExtractor>();
    public String default_value = "";

    public String toString() {
        return String.format("{name:%s, is_required:%s, extractor_type:%s, extractor_args_string:%s, default_value:%s}", name, is_required, extractor_type, extractor_args, default_value);
    }

    public static class SubExtractor {
        public String extractor_type;
        public List<String> extractor_args;
    }
}
