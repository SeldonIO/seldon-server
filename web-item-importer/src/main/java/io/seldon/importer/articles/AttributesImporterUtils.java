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
package io.seldon.importer.articles;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class AttributesImporterUtils {

	public static Set<String> getTags(Document articleDoc, String tagsCssSelector, String title) {
		Set<String> tagSet = new HashSet<String>();

		if (StringUtils.isNotBlank(tagsCssSelector)) {
			Elements tagsElements = articleDoc.select(tagsCssSelector);
			Element tagsElement = tagsElements.first();
			List<String> tagsParts;
			if ((tagsElement != null) && (tagsElement.attr("content") != null)
					&& (StringUtils.isNotBlank(tagsElement.attr("content")))) {
				tagsParts = AttributesImporterUtils.getTagsPartsFromSingleElement(tagsElement);
			} else {
				tagsParts = AttributesImporterUtils.getTagsPartsFromMultipleElement(tagsElements);

			}
			List<String> extraTagsParts = AttributesImporterUtils.createExtraTagsPartsFromTitle(title, tagsParts);
			tagSet.addAll(tagsParts);
			tagSet.addAll(extraTagsParts);
		}

		return tagSet;
	}

	// for like -tagsSelector "head > meta[name=keywords]"
	public static List<String> getTagsPartsFromSingleElement(Element tagsElement) {
		String tagsRaw = tagsElement.attr("content");
		String[] parts = tagsRaw.split(",");
		for (int i = 0; i < parts.length; i++)
			parts[i] = parts[i].trim().toLowerCase();

		List<String> tagsParts = (parts != null) ? new ArrayList<String>(Arrays.asList(parts))
				: new ArrayList<String>();

		return tagsParts;
	}

	// for like -tagsSelector "section[id=tags] > a"
	public static List<String> getTagsPartsFromMultipleElement(Elements tagsElements) {
		List<String> tagsParts = new ArrayList<String>();

		for (Element e : tagsElements) {
			String tag = e.text();
			tag = StringUtils.strip(tag);
			tag = tag.toLowerCase();
			tagsParts.add(tag);
		}

		return tagsParts;
	}

	public static List<String> createExtraTagsPartsFromTitle(String title, List<String> tagsParts) {
		List<String> tileParts = new ArrayList<String>();

		boolean haveTitle = StringUtils.isNotBlank(title);
		String titleLower = title.toLowerCase();
		String last_part = null;
		for (String part : tagsParts) {
			if (StringUtils.isNotBlank(part)) {
				if (haveTitle && (last_part != null) && StringUtils.isNotBlank(last_part)) {
					String phrase = last_part + " " + part;
					if (titleLower.contains(phrase))
						tileParts.add(phrase.toLowerCase());
				}
			}
			last_part = part;
		}

		return tileParts;
	}
	
	public static void logResult(Logger logger, ItemProcessResult itemProcessResult) {
		
		String v = "none";
		ObjectMapper mapper = new ObjectMapper();
		try {
			v = mapper.writeValueAsString( itemProcessResult );
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("ItemProcessResult: "+v);
	}
	
	public static String getBaseUrl(String url) throws Exception {
	    URL aURL = new URL(url);
	    
	    String protocol = aURL.getProtocol();
	    String host = aURL.getHost();
	    String path = aURL.getPath();
	    
	    String baseUrl = String.format("%s://%s%s", protocol,host,path);
	    return baseUrl;
	}
}
