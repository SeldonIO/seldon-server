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

import io.seldon.client.DefaultApiClient;
import io.seldon.client.beans.ItemBean;
import io.seldon.client.exception.ApiException;
import io.seldon.importer.articles.category.CategoryExtractor;
import io.seldon.utils.CollectionTools;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

public class FileItemAttributesImporter {

	private static Logger logger = Logger.getLogger(FileItemAttributesImporter.class.getName());
	
	@Argument(alias = "n", description = "How many items to import", required = false)
	static Integer numItems = 500;
		
	@Argument(alias = "i", description = "Interval in secs between runs", required = false)
	static Integer intervalSecs = 600;
	
	@Argument(alias = "gt", description = "Timeout on article http GET", required = false)
	static Integer httpGetTimeout = 2000;
	
	@Argument(alias = "api-url", description = "API Endpoint", required = true)
	static String apiUrl;
	
	@Argument(alias = "consumer-key", description = "Consumer Key", required = true)
	static String consumerKey;
	
	@Argument(alias = "consumer-secret", description = "Consumer Secret", required = true)
	static String consumerSecret;

	
	
	@Argument(alias = "it", description = "Item type", required = false, delimiter = ",")
	static Integer[] itemTypes = new Integer[] {0,1};
	
	/*
	 * CSS Selector arguments
	 */
	
	@Argument(alias = "imageSelector", description = "Image CSS Selector", required = false)
	static String imageCssSelector = "head > meta[property=og:image]";

	@Argument(alias = "titleSelector", description = "Title CSS Selector", required = false)
	static String titleCssSelector = "head > meta[property=og:title]";

	@Argument(alias = "leadTextSelector", description = "Lead Text CSS Selector", required = false)
	static String leadTextCssSelector = "";

	
	@Argument(alias = "textSelector", description = "Article text CSS Selector", required = false)
	static String textCssSelector;

	@Argument(alias = "tagsSelector", description = "Tags CSS Selector", required = false)
	static String tagsCssSelector = "head > meta[name=keywords]";

	@Argument(alias = "categorySelector", description = "Category CSS Selector", required = false)
	static String categoryCssSelector;
	
	@Argument(alias = "subCategorySelector", description = "Sub Category CSS Selector", required = false)
	static String subCategoryCssSelector;

	
	@Argument(alias = "linkSelector", description = "URL link CSS Selector", required = false)
	static String linkCssSelector;

	@Argument(alias = "publishDateSelector", description = "publish date CSS Selector", required = false)
	static String publishDateCssSelector;

	
	@Argument(alias = "urls", description = "File containing list of URLs", required = true)
	static String urlFile;
	
	@Argument(alias = "minFetch", description = "Min time between url requests in msecs", required = false)
	static Integer minFetchGapMsecs = 500;
	
	/*
	 * Arguments to allow success even if we don't find some elements
	 */
	
	@Argument(alias = "noimage", description = "is it ok not to find an image", required = false)
	static boolean imageNotNeeded = false;
	
	@Argument(alias = "nocategory", description = "is it ok not to find a category", required = false)
	static boolean categoryNotNeeded = false;

	/*
	 * Argument for using Domain
	 */
	@Argument(alias = "needdomain", description = "Should doamin be used as an attrinute", required = false)
	static boolean domainIsNeeded = false;
	
	/*
	 * Defaults
	 */
	
	@Argument(alias = "defImage", description = "Default image url", required = false)
	static String defImageUrl;

	
	@Argument(alias = "categoryPrefix", description = "The prefix for the supplied category extractor - will be io.seldon.importer.articles.category.<Prefix>CategoryExtractor", required = false)
	static String categoryClassPrefix = "GeneralFirst";
	
	@Argument(alias = "subCategoryPrefix", description = "The prefix for the supplied sub category extractor - will be io.seldon.importer.articles.category.<Prefix>SubCategoryExtractor", required = false)
	static String subCategoryClassPrefix = ""; //"GeneralAll";

	@Argument(alias = "t", description = "For testing, will not update", required = false)
	static boolean testmode = false;
	
	static int API_TIMEOUT = 10000;
	static String ATTR_IMG_NAME = "img_url";
	static final String ATTR_CATEGORIES = "categories";
	static final String ATTR_TITLE = "title";
	
	static final String CONTENT_TYPE_ARTICLE_VALID = "article";
	static final String CONTENT_TYPE_ARTICLE_INVALID = "old_article";
	
	static final int TYPE_NOT_VALID = 2;
	static final int TYPE_NOT_SET = 0;
	static final int TYPE_VALID = 1;
	
	static final String UNVERIFIED_CONTENT_TYPE = "unverified_article";
	static final String VERIFIED_CONTENT_TYPE = "article";


    //field
	static String CONTENT_TYPE = "content_type";
	static String TITLE = "title";
    static String CATEGORY = "category";
    static String SUBCATEGORY = "subcategory";
    static String IMG_URL = "img_url";
    static String DESCRIPTION = "description";
    static String TAGS = "tags";
    static String LEAD_TEXT = "leadtext";
	static String LINK = "link";
	static String PUBLISH_DATE = "published_date";
	static String DOMAIN = "domain";

	private int total_item_processed_count = 0;
	private int total_item_succeded_count = 0;

	private FailFast failFast = null;
	
	DefaultApiClient client;

	static long lastUrlFetchTime = 0;
	
	public FileItemAttributesImporter(DefaultApiClient client) {
		this.client = client;
	}
	
	public void run() throws InterruptedException
	{
		logger.info("Starting...");
		logger.info("Processing recent urls...");
		int updates = process();
		logger.info("Processed with "+updates+" updates");
		logger.info("Processed urls...Finished");
		if (failFast != null) {
			failFast.stopChecking(); // We are exiting normally so no need to check the main thread is going to die
		}
	}
	
	public static String getUrlEncodedString(String input)
	{
		URL url = null;
		try
		{
			url = new URL(input);
			
			URI uri = new URI(
			        url.getProtocol(), 
			        url.getHost(), 
			        url.getPath(),
			        url.getQuery(),
			        null);

			
			String encoded = uri.toASCIIString();
			
			return encoded;
			
		}
		catch(MalformedURLException mue)
		{
			logger.error("Malformed url "+input);
			return null;
		} 
		catch (URISyntaxException e) 
		{
			logger.error("Failed to tranform url into uri ",e);
			return null;
		}
	}


	public int process() 
	{
		int updates = 0;
		try
		{
			Map<String,ItemBean> itemMap = new HashMap<String,ItemBean>();
			List<ItemBean> items = new ArrayList<ItemBean>();
			for(int i=0;i<itemTypes.length;i++)
			{
				List<ItemBean> itemsForType = client.getItems(numItems, itemTypes[i], true, "last_action"); 
				logger.info("Adding "+itemsForType.size()+" items for item type "+itemTypes[i]);
				items.addAll(itemsForType);
			}

			logger.info("Got "+items.size()+" items from API");
			for(ItemBean item : items)
				itemMap.put(item.getId(), item);
			
			BufferedReader reader = new BufferedReader(new FileReader(urlFile));
			String url;
			int count = 0;
			while ((url = reader.readLine()) != null)
			{
				count++;
				ItemBean item = itemMap.get(url);
				String contentType = null;
				if (item == null)
					item = new ItemBean(url, "", 1);
				else
					contentType = item.getAttributesName().get(ItemAttributesImporter.CONTENT_TYPE);
				if(item.getType() == ItemAttributesImporter.TYPE_NOT_SET || (contentType == null || ItemAttributesImporter.UNVERIFIED_CONTENT_TYPE.equals(contentType))) 
				{
					total_item_processed_count++;
					logger.info("Looking at item "+count);
					System.out.println("Item => "+item.toString());
					boolean imported = false;
					try 
					{
						String category = null;
						if (item.getAttributesName() != null)
							category = item.getAttributesName().get(ItemAttributesImporter.CATEGORY);
						
						
						
						Map<String,String> attributes = getAttributes(item.getId(),category);
						if(attributes != null) 
						{
							updates++;
							total_item_succeded_count++;
							item.setName(attributes.get(ItemAttributesImporter.ATTR_TITLE));
							item.setAttributesName(attributes);
							item.setType(ItemAttributesImporter.TYPE_VALID);
							item.setFirst_action(new Date());
							item.setLast_action(new Date());
							if (!testmode) { 
								client.updateItem(item);
							} else {
								logger.info("TESTMODE skipping update");
							}
							imported = true;
						}
					}
					catch(Exception e) {
						logger.warn("Article:" + item.getId() + " error.",e);
					}
					
					String updated_amount_string = String.format("[%d/%d %.0f%%]", total_item_succeded_count,total_item_processed_count, ((((double)total_item_succeded_count/(double)total_item_processed_count))*100));	
					if(imported) { 
						logger.info("Article : " + item.getId() + " import - OK "+updated_amount_string);
						logger.info("Item : " + item);
					}
					else {
						logger.info("Article : " + item.getId() + " import - NOT OK "+updated_amount_string);
					}
				}
				else {
					logger.info("Article : " + item.getId() + " SKIPPED");
				}
			}
			reader.close();
		}
		catch (ApiException e) 
		{
			logger.error("Failed api call",e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("io exception",e);
		}
		return updates;
	}



	public static  Map<String, String> getAttributes(String url,String existingCategory) {
		ItemProcessResult itemProcessResult = new ItemProcessResult();
		itemProcessResult.client_item_id = url;
		itemProcessResult.extraction_status = "EXTRACTION_FAILED";
		
		logger.info("Trying to get attributes for "+url);
		Map<String,String> attributes = null;
		String title="";
		String category="";
		String subCategory = "";
		String img_url="";
		String description="";
		String tags = "";
		String leadtext = "";
		String link = "";
		String publishDate = "";
		String domain = "";
		try {
			long now = System.currentTimeMillis();
			long timeSinceLastRequest = now - lastUrlFetchTime;
			if (timeSinceLastRequest < minFetchGapMsecs)
			{
				long timeToSleep = minFetchGapMsecs - timeSinceLastRequest;
				logger.info("Sleeping "+timeToSleep+"msecs as time since last fetch is "+timeSinceLastRequest);
				Thread.sleep(timeToSleep);
			}
			Document articleDoc = Jsoup.connect(url).userAgent("SeldonBot/1.0").timeout(httpGetTimeout).get();
			lastUrlFetchTime = System.currentTimeMillis();
			//get IMAGE URL
			if (StringUtils.isNotBlank(imageCssSelector))
			{
				Element imageElement = articleDoc.select(imageCssSelector).first();
				if (imageElement != null)
				{
					if (imageElement.attr("content") != null) {
						img_url = imageElement.attr("content");
					}
					if (StringUtils.isBlank(img_url) && imageElement.attr("src") != null) {
						img_url = imageElement.attr("src");
					}
					if (StringUtils.isBlank(img_url) && imageElement.attr("href") != null) {
						img_url = imageElement.attr("href");
					}

				}
			}
			if (StringUtils.isBlank(img_url) && StringUtils.isNotBlank(defImageUrl))
			{
				logger.info("Setting image to default: "+defImageUrl);
				img_url = defImageUrl;
			}
			img_url = StringUtils.strip(img_url);
			
			//get TITLE
			if (StringUtils.isNotBlank(titleCssSelector))
			{
				Element titleElement = articleDoc.select(titleCssSelector).first();
				if (titleElement != null && titleElement.attr("content") != null) {
					title = titleElement.attr("content");
				}
			}
			
			//get Lead Text
			if (StringUtils.isNotBlank(leadTextCssSelector))
			{
				Element leadElement = articleDoc.select(leadTextCssSelector).first();
				if (leadElement != null && leadElement.attr("content") != null) {
					leadtext = leadElement.attr("content");
				}
			}
			
			//get publish date
			if (StringUtils.isNotBlank(publishDateCssSelector))
			{
				//2013-01-21T10:40:55Z
				Element pubElement = articleDoc.select(publishDateCssSelector).first();
				if (pubElement != null && pubElement.attr("content") != null) {
					String pubtext = pubElement.attr("content");
					SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH);
				    Date result =  null;
				    try{
				    	result = df.parse(pubtext);
				    }
				    catch (ParseException e)
				    {
				    	logger.info("Failed to parse date withUTC format "+pubtext);
				    }
				    //try a simpler format
				    df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
				    try{
				    	result = df.parse(pubtext);
				    }
				    catch (ParseException e)
				    {
				    	logger.info("Failed to parse date "+pubtext);
				    }

				    if (result != null)
				    	publishDate = dateFormatter.format(result);
				    else
				    	logger.error("Failed to parse date "+pubtext);
				}
			}

			
			//get Link
			if (StringUtils.isNotBlank(linkCssSelector))
			{
				Element linkElement = articleDoc.select(linkCssSelector).first();
				if (linkElement != null && linkElement.attr("content") != null) {
					link = linkElement.attr("content");
				}
			}
			
			//get CONTENT
			if (StringUtils.isNotBlank(textCssSelector))
			{
				Element descriptionElement = articleDoc.select(textCssSelector).first();
				if (descriptionElement != null)
					description = Jsoup.parse(descriptionElement.html()).text();
			}
			
			
			//get TAGS
			Set<String> tagSet = AttributesImporterUtils.getTags(articleDoc, tagsCssSelector, title);
			
			if (tagSet.size() > 0)
				tags = CollectionTools.join(tagSet, ",");
			
			//get CATEGORY - client specific
			if (StringUtils.isNotBlank(categoryCssSelector))
			{
				Element categoryElement = articleDoc.select(categoryCssSelector).first();
				if (categoryElement != null && categoryElement.attr("content") != null) {
					category = categoryElement.attr("content");
					if (StringUtils.isNotBlank(category))
						category = category.toUpperCase();
				}
			}
			else if (StringUtils.isNotBlank(categoryClassPrefix))
			{
				String className = "io.seldon.importer.articles.category."+categoryClassPrefix+"CategoryExtractor";
				Class<?> clazz = Class.forName(className);
				Constructor<?> ctor = clazz.getConstructor();
				CategoryExtractor extractor = (CategoryExtractor) ctor.newInstance();
				category = extractor.getCategory(url, articleDoc);
			}
			
			
			//get Sub CATEGORY - client specific
			if (StringUtils.isNotBlank(subCategoryCssSelector))
			{
				Element subCategoryElement = articleDoc.select(subCategoryCssSelector).first();
				if (subCategoryElement != null && subCategoryElement.attr("content") != null) {
					subCategory = subCategoryElement.attr("content");
					if (StringUtils.isNotBlank(subCategory))
						subCategory = category.toUpperCase();
				}
			}
			else if (StringUtils.isNotBlank(subCategoryClassPrefix))
			{
				String className = "io.seldon.importer.articles.category."+subCategoryClassPrefix+"SubCategoryExtractor";
				Class<?> clazz = Class.forName(className);
				Constructor<?> ctor = clazz.getConstructor();
				CategoryExtractor extractor = (CategoryExtractor) ctor.newInstance();
				subCategory = extractor.getCategory(url, articleDoc);
			}

			// Get domain
			if (domainIsNeeded) {
				domain = getDomain(url);
			}
			
			if(StringUtils.isNotBlank(title) 
					&& (imageNotNeeded || StringUtils.isNotBlank(img_url))
					&& (categoryNotNeeded || StringUtils.isNotBlank(category))
					&& (!domainIsNeeded || StringUtils.isNotBlank(domain))
					) 
			{
				attributes = new HashMap<String,String>();
				attributes.put(TITLE,title);
				if (StringUtils.isNotBlank(category))
					attributes.put(CATEGORY, category);
				if (StringUtils.isNotBlank(subCategory))
					attributes.put(SUBCATEGORY, subCategory);
				if (StringUtils.isNotBlank(link))
					attributes.put(LINK, link);
				if (StringUtils.isNotBlank(leadtext))
					attributes.put(LEAD_TEXT, leadtext);
				if (StringUtils.isNotBlank(img_url))
					attributes.put(IMG_URL, img_url);
				if (StringUtils.isNotBlank(tags))
					attributes.put(TAGS, tags);
				attributes.put(CONTENT_TYPE, VERIFIED_CONTENT_TYPE);
				if (StringUtils.isNotBlank(description))
					attributes.put(DESCRIPTION,description);
				if (StringUtils.isNotBlank(publishDate))
					attributes.put(PUBLISH_DATE, publishDate);
				if (StringUtils.isNotBlank(domain))
					attributes.put(DOMAIN, domain);
				System.out.println("Item: "+url+"; Category: "+category);
				itemProcessResult.extraction_status = "EXTRACTION_SUCCEEDED";
			}
			else {
				logger.warn("Failed to get title for article "+url);
				logger.warn("[title="+title+", img_url="+img_url+", category="+category+", domain="+domain+"]");
			}
			
			{ // check for failures for the log result
				if (StringUtils.isBlank(title)) {
					itemProcessResult.attrib_failure_list = itemProcessResult.attrib_failure_list + ((StringUtils.isBlank(itemProcessResult.attrib_failure_list))?"":",")+ "title";
				}
				if (!imageNotNeeded && StringUtils.isBlank(img_url)) {
					itemProcessResult.attrib_failure_list = itemProcessResult.attrib_failure_list + ((StringUtils.isBlank(itemProcessResult.attrib_failure_list))?"":",")+ "img_url";
				}
				if (!categoryNotNeeded && StringUtils.isBlank(category)) {
					itemProcessResult.attrib_failure_list = itemProcessResult.attrib_failure_list + ((StringUtils.isBlank(itemProcessResult.attrib_failure_list))?"":",")+ "category";
				}
			}
		}
		catch(Exception e) {
			logger.warn("Article: " + url + ". Attributes import FAILED",e);
			itemProcessResult.error = e.toString();
		}
		
		AttributesImporterUtils.logResult(logger, itemProcessResult);
		
		return attributes;
	}

	

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		
		FailFast failFast = new FailFast(Thread.currentThread());
		{ // Fail Fast thread
			Thread fail_fast_thread = new Thread(failFast);
			fail_fast_thread.setName("fail_fast_thread");
			fail_fast_thread.start();
		}
		
		try 
		{
			Args.parse(FileItemAttributesImporter.class, args);

			DefaultApiClient client = new DefaultApiClient(apiUrl,consumerKey,consumerSecret,API_TIMEOUT);
			
			FileItemAttributesImporter fixer = new FileItemAttributesImporter(client);
			
			fixer.setFailFast(failFast);
			fixer.run();
			
		} 
		catch (IllegalArgumentException e) 
		{
			e.printStackTrace();
			Args.usage(FileItemAttributesImporter.class);
		}	
	}
	
	/**
	 * 
	 * @param url The domain to extract the domain from.
	 * @return The domain or UNKOWN_DOMAN if unable to use url.
	 */
	private static String getDomain(String url) {
		String retVal = "UNKOWN_DOMAN";
		if (!url.startsWith("http") && !url.startsWith("https")) {
			url = "http://" + url;
		}
		URL netUrl = null;
		try {
			netUrl = new URL(url);
		} catch (MalformedURLException e) {
			logger.warn("Failed to get domain for "+ url);
		}
		if (netUrl != null) {
			String host = netUrl.getHost();
			retVal = host;
		}

		return retVal;
	}

	private void setFailFast(FailFast failFast) {
		this.failFast = failFast;
	}
}
