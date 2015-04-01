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
import io.seldon.importer.articles.dynamicextractors.AttributeDetail;
import io.seldon.importer.articles.dynamicextractors.AttributeDetailList;
import io.seldon.importer.articles.dynamicextractors.DynamicExtractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

public class GeneralItemAttributesImporter {

	private static Logger logger = Logger.getLogger(GeneralItemAttributesImporter.class.getName());

	private static enum OperationMode {
		OPERATION_MODE_ITEM_IMPORTER,
		OPERATION_MODE_FILE_IMPORTER
	}
	
	private static OperationMode opMode = OperationMode.OPERATION_MODE_ITEM_IMPORTER; // set a default mode
	
	@Argument(alias = "n", description = "How many items to import", required = false)
	static Integer numItems = 500;
		
	@Argument(alias = "i", description = "Interval in secs between runs", required = false)
	static Integer intervalSecs = 600;
	
	@Argument(alias = "gt", description = "Timeout on article http GET", required = false)
	static Integer httpGetTimeout = 10000;
	
	@Argument(alias = "minFetch", description = "Min time between url requests in msecs", required = false)
	static Integer minFetchGapMsecs = 500;
	
	@Argument(alias = "api-url", description = "API Endpoint", required = false)
	static String apiUrl;
	
	@Argument(alias = "consumer-key", description = "Consumer Key", required = false)
	static String consumerKey;
	
	@Argument(alias = "consumer-secret", description = "Consumer Secret", required = false)
	static String consumerSecret;

	@Argument(alias = "cf", description = "Client Id Filter - only process client ids starting with this string", required = false)
	static String clientIdFilter;

	@Argument(alias = "urls", description = "File containing list of URLs", required = false)
	static String urlFile = null;

   @Argument(alias = "banner-id-name", description = "This will modify the client id to 'banner-XXXX", required = false)
    static String bannerIdName = null;
   
   @Argument(alias = "banner-valid-type", description = "The type(int) to use then an item is valid", required = false)
   static String bannerValidType = null;

   @Argument(alias = "banner-invalid-type", description = "The type(int) to use then an item is invalid", required = false)
   static String bannerInValidType = null;
   
   @Argument(alias = "banner-item-log-file", description = "Log imported banner items (id and baseurl) to file", required = false)
   static String bannerItemLogFile;

   @Argument(alias = "add-base-url-attribute", description = "Add an additional attribute for the baseurl", required = false)
   static boolean addBaseUrlAttribute = false;

   @Argument(alias = "content-type-override", description = "Override the default content_type", required = false)
   static String contentTypeOverride = null;
   
   @Argument(alias = "invalid-content-type-override", description = "Override the default content_type", required = false)
   static String invalidContentTypeOverride = null;


	
	@Argument(alias = "it", description = "Item type", required = false, delimiter = ",")
	static Integer[] itemTypes = new Integer[] {0,1};

	
	@Argument(alias = "t", description = "For testing, will not update", required = false)
	static boolean testmode = false;
	
	@Argument(alias = "j", description = "Is javascript in page supported", required = false)
	static boolean jsSupport = false;
	
	@Argument(alias = "attributes_config_file", description = "Attributes details", required = false)
	static String attributesConfigFile;
	
	@Argument(alias = "test_url", description = "Check url only", required = false)
	static String testUrl = null;
	
	static int API_TIMEOUT = 10000;
	static String ATTR_IMG_NAME = "img_url";
	static final String ATTR_CATEGORIES = "categories";
	static final String ATTR_LINK = "link";
	static final String ATTR_TITLE = "title";
	
	static final String CONTENT_TYPE_ARTICLE_VALID = "article";
	static final String CONTENT_TYPE_ARTICLE_INVALID = "old_article";
	
	public static final int TYPE_NOT_VALID = 2;
	public static final int TYPE_NOT_SET = 0;
	public static final int TYPE_VALID = 1;
	
	static final String UNVERIFIED_CONTENT_TYPE = "unverified_article";
	static final String VERIFIED_CONTENT_TYPE = "article";


    //field
	public static String CONTENT_TYPE = "content_type";
	static String CATEGORY = "category";

	private int total_item_processed_count = 0;
	private int total_item_succeded_count = 0;

	private FailFast failFast = null;
	
	DefaultApiClient client;
	static long lastUrlFetchTime = 0;
	
	private static List<AttributeDetail> attribute_detail_list = null;
	
	public GeneralItemAttributesImporter(DefaultApiClient client) {
		this.client = client;
	}
	
	public void run(UrlFetcher urlFetcher) throws InterruptedException
	{
		boolean keepGoing = true;
		logger.info("Starting...");
		logger.info("opMode: "+opMode.toString());

		while(keepGoing)
		{
			logger.info("Processing recent urls...");
			int updates = 0;
			
			if (opMode == OperationMode.OPERATION_MODE_ITEM_IMPORTER ) {		
					updates = process_as_item_importer(urlFetcher);
			} else {
				updates = process_as_file_importer(urlFetcher);
			}
			
			logger.info("Processed with "+updates+" updates");
			
			if (opMode == OperationMode.OPERATION_MODE_FILE_IMPORTER) {
				keepGoing = false;
				logger.info("Processed urls...Finished");
				if (failFast != null) {
					failFast.stopChecking(); // We are exiting normally so no need to check the main thread is going to die
				}
			} else {
				logger.info("Processed urls..sleeping...");
				Thread.sleep(intervalSecs * 1000);
			}
		}
		
		{ // If we get here then we should terminate. Force the termination as sometimes when as file importer it hangs due to open connections
			logger.info("End of processing so terminating process.");
			{ // Flush the file log appenders
				Enumeration<?> appenders = logger.getAllAppenders();
				while (appenders.hasMoreElements()) {
					Appender  appender = (Appender)appenders.nextElement();
					if (appender instanceof FileAppender) {
						FileAppender fileAppender = (FileAppender)appender;
						fileAppender.setImmediateFlush(true);
					}
				}
			}
			System.exit(0); // We've finished processing
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


	public int process_as_item_importer(UrlFetcher urlFetcher)
	{
		int updates = 0;
		try
		{
			int count = 0;
			int foundItems = 0;
			List<ItemBean> items = new ArrayList<ItemBean>();
			for(int i=0;i<itemTypes.length;i++)
			{
				List<ItemBean> itemsForType = client.getItems(numItems, itemTypes[i], true, "last_action"); 
				logger.info("Adding "+itemsForType.size()+" items for item type "+itemTypes[i]);
				items.addAll(itemsForType);
			}
			foundItems = items.size();
			for(ItemBean item : items)
			{
				count++;
				String contentType = item.getAttributesName().get(CONTENT_TYPE);
				if(item.getType() == TYPE_NOT_SET || (contentType == null || UNVERIFIED_CONTENT_TYPE.equals(contentType))) {
					total_item_processed_count++;
					logger.info("Looking at item "+count+"/"+foundItems);
					System.out.println("Item => "+item.toString());
					boolean imported = false;
					if (clientIdFilter != null && !item.getId().startsWith(clientIdFilter))
					{
						logger.info("Skipping as does not match client id filter "+item.getId());
						continue;
					}
					else if (item.getId().startsWith("file://"))
					{
						logger.warn("Ignoring bad url: "+item.getId());
						continue;
					}
					try {
						
						Map<String,String> attributes = getAttributes(urlFetcher, item.getId(),item.getAttributesName().get(CATEGORY));
						if(attributes != null) {
							updates++;
							total_item_succeded_count++;
							item.setName(attributes.get(GeneralItemAttributesImporter.ATTR_TITLE));
							item.setAttributesName(attributes);
							item.setType(TYPE_VALID);
							mofidyItemId(item); // check the -bannerIdName flag
							if (!testmode) { 
								client.updateItem(item);
							} else {
								logger.info("TESTMODE skipping update");
								logger.info("Item Details: " + item);
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
		} 
		catch (ApiException e) 
		{
			logger.error("Failed api call",e);
		}
		return updates;
	}

	public int process_as_file_importer(UrlFetcher urlFetcher) 
	{
		int updates = 0;
		try
		{
			
			BufferedReader reader = new BufferedReader(new FileReader(urlFile));
			String url;
			int count = 0;
			while ((url = reader.readLine()) != null)
			{
				count++;
				ItemBean item = new ItemBean(url, "", 1);
				total_item_processed_count++;
				logger.info("Looking at item "+count);
				System.out.println("Item => "+item.toString());
				boolean imported = false;
				try 
				{
					String category = null;
					if (item.getAttributesName() != null)
						category = item.getAttributesName().get(GeneralItemAttributesImporter.CATEGORY);
						
						
						
					Map<String,String> attributes = getAttributes(urlFetcher,item.getId(),category);
					if(attributes != null) 
					{
						updates++;
						total_item_succeded_count++;
						item.setName(attributes.get(GeneralItemAttributesImporter.ATTR_TITLE));
						item.setAttributesName(attributes);
						item.setType(GeneralItemAttributesImporter.TYPE_VALID);
						item.setFirst_action(new Date());
						item.setLast_action(new Date());
						mofidyItemId(item); // check the -bannerIdName flag
						invalidateUsingBannerItemLogFile(item); // check the -bannerItemLogFile flag
						if (!testmode) { 
							client.updateItem(item);
						} else {
							logger.info("TESTMODE skipping update");
							logger.info("Item Details: " + item);
						}
						imported = true;
					} else {
					    // item failed import but still need to invalidate if banner item
                            if (addBaseUrlAttribute) {
                                Map<String,String> attributes_override = new HashMap<String,String>();
                                String baseUrl = AttributesImporterUtils.getBaseUrl(url);
                                attributes_override.put("baseurl", baseUrl);
                                item.setAttributesName(attributes_override);
                                invalidateUsingBannerItemLogFile(item); // check the -bannerItemLogFile flag
                            }
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
			reader.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("io exception",e);
		}
		return updates;
	}
	
	public static  Map<String, String> getAttributes(UrlFetcher urlFetcher, String url,String existingCategory) {
		ItemProcessResult itemProcessResult = new ItemProcessResult();
		itemProcessResult.client_item_id = url;
		itemProcessResult.extraction_status = "EXTRACTION_FAILED";
		
		logger.info("Trying to get attributes for "+url);
		Map<String,String> attributes = null;
		try {
			long now = System.currentTimeMillis();
			long timeSinceLastRequest = now - lastUrlFetchTime;
			if (timeSinceLastRequest < minFetchGapMsecs)
			{
				long timeToSleep = minFetchGapMsecs - timeSinceLastRequest;
				logger.info("Sleeping "+timeToSleep+"msecs as time since last fetch is "+timeSinceLastRequest);
				Thread.sleep(timeToSleep);
			}
			
			String page = urlFetcher.getUrl(url); // fetch the article page
			Document articleDoc = Jsoup.parse(page);
			
			lastUrlFetchTime = System.currentTimeMillis();
			
			attributes = new HashMap<String,String>();
			
			if (attribute_detail_list!=null) {
				
				for (AttributeDetail attributeDetail: attribute_detail_list) {
					String attrib_name = attributeDetail.name;
					String attrib_value = null;
					String attrib_default_value = attributeDetail.default_value;
					
					String extractorType = attributeDetail.extractor_type;
                    if (StringUtils.isNotBlank(extractorType)) {
						DynamicExtractor extractor = DynamicExtractor.build(extractorType);
						attrib_value = extractor.extract(attributeDetail, url, articleDoc);
					}
					
					attrib_name = (attrib_name!=null) ? StringUtils.strip( attrib_name ) : "";
					attrib_value = (attrib_value!=null) ? StringUtils.strip( attrib_value ) : "";
					attrib_default_value = (attrib_default_value!=null) ? StringUtils.strip( attrib_default_value ) : "";
					
					// Use the default value if necessary
					if ( StringUtils.isBlank(attrib_value) && StringUtils.isNotBlank(attrib_default_value)) {
						attrib_value = attrib_default_value;
					}
					
					if (StringUtils.isNotBlank(attrib_name) && StringUtils.isNotBlank(attrib_value)) {
						attributes.put(attrib_name, attrib_value);
					}
				}
				
				// check if the import was ok
				boolean required_import_ok = true;
				for (AttributeDetail attributeDetail: attribute_detail_list) {
					if (attributeDetail.is_required && (!attributes.containsKey(attributeDetail.name))) {
						required_import_ok = false;
						itemProcessResult.attrib_failure_list = itemProcessResult.attrib_failure_list + ((StringUtils.isBlank(itemProcessResult.attrib_failure_list))?"":",")+ attributeDetail.name;
					}
				}
				
				if(required_import_ok) 
				{
				    if (contentTypeOverride == null) {
				        attributes.put(CONTENT_TYPE, VERIFIED_CONTENT_TYPE);
				    } else {
				        attributes.put(CONTENT_TYPE, contentTypeOverride);
				    }
					if (addBaseUrlAttribute) {
					    String baseUrl = AttributesImporterUtils.getBaseUrl(url);
					    attributes.put("baseurl", baseUrl);
					}
					itemProcessResult.extraction_status = "EXTRACTION_SUCCEEDED";
				}
				else {
					attributes = null;
					logger.warn("Failed to get needed attributes for article "+url);
				}
				
			}
			
			
		}
		catch(Exception e) {
			logger.error("Article: " + url + ". Attributes import FAILED",e);
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
			Args.parse(GeneralItemAttributesImporter.class, args);
			
			UrlFetcher urlFetcher = null;
			{ // setup the correct UrlFetcher
			    if (jsSupport) {
			        urlFetcher = new JsSupporedUrlFetcher(httpGetTimeout);
			    } else {
			        urlFetcher = new SimpleUrlFetcher(httpGetTimeout);
			    }
			}
			
			{ // Determine opMode by checking for urlFile
				if (urlFile !=null) {
					opMode = OperationMode.OPERATION_MODE_FILE_IMPORTER;
				}
			}
			{ // setup the attribute_detail_list
				attribute_detail_list = getAttributeDetailList(attributesConfigFile);
			}
			if (testUrl!=null){
				test_url_and_exit(urlFetcher, testUrl);
			}


			DefaultApiClient client = new DefaultApiClient(apiUrl,consumerKey,consumerSecret,API_TIMEOUT);
			
			GeneralItemAttributesImporter fixer = new GeneralItemAttributesImporter(client);

			fixer.setFailFast(failFast);
			fixer.run(urlFetcher);
			
		} 
		catch (IllegalArgumentException e) 
		{
			e.printStackTrace();
			Args.usage(GeneralItemAttributesImporter.class);
		} catch (Exception e) {
			e.printStackTrace();
			Args.usage(GeneralItemAttributesImporter.class);
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
	
	private void mofidyItemId(ItemBean item) throws Exception {
	    if (bannerIdName != null) {
	        if (bannerValidType == null) {
	            throw new Exception("using -bannerIdName but bannerValidType is null");
	        }
	        String item_id_current = item.getId();
	        String id_to_use = item.getAttributesName().get( bannerIdName );
	        String item_id_updated = "banner-"+id_to_use;
	        item.setId( item_id_updated );
	        item.setType(Integer.parseInt(bannerValidType));
	        logger.info(String.format("updated item id [%s] -> [%s]", item_id_current, item_id_updated));
	    }
	}
	
	private void invalidateUsingBannerItemLogFile(ItemBean item) throws Exception {
	    if (bannerItemLogFile != null) {
	        
	        if (bannerInValidType == null) {
	            throw new Exception("using -bannerItemLogFile but bannerInValidType is null");
	        }
	        
	        final String current_item_id = item.getId();
	        final String current_item_baseurl = item.getAttributesName().get( "baseurl" );
	        
	        { // make sure we log the current item if it was modified
	            if (!current_item_id.equals(current_item_baseurl)) { // if the currrent's id and baseurl are the same - means it wasnt imported and it set with an id, so dont write it out
                        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter( bannerItemLogFile, true)));
                        out.println( current_item_id+"|"+current_item_baseurl);
                        out.close();
	            }
	        }
	        
	        File f = new File(bannerItemLogFile);
	        if(f.exists() && !f.isDirectory()) {
                Set<String> itemList = new HashSet<String>();
                BufferedReader br = new BufferedReader(new FileReader(bannerItemLogFile));
                String line;
                while ((line = br.readLine()) != null) {
                    line = StringUtils.trim(line);
                    if (line.length()>0) {
                        itemList.add(line);
                    }
                }
                br.close();
                
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter( bannerItemLogFile, false)));
                for (String loggedItem: itemList) {
                    
                    String logged_item_id = null;
                    String logged_item_baseurl = null;
                    {
                        String[] line_items = StringUtils.split(loggedItem, '|');
                        if (line_items.length == 2) {
                                logged_item_id = line_items[0];
                                logged_item_baseurl = line_items[1];
                        }
                    }
                    if ((logged_item_id!=null) &&(logged_item_baseurl!=null)) {
                    
                            if (logged_item_baseurl.equalsIgnoreCase(current_item_baseurl) && !logged_item_id.equalsIgnoreCase(current_item_id)) {
                                ItemBean itemToInvalidate = new ItemBean(logged_item_id, "", Integer.parseInt(bannerInValidType));
                                if (invalidContentTypeOverride != null) {
                                    Map<String,String> attributes_override = new HashMap<String,String>();
                                    attributes_override.put("content_type", invalidContentTypeOverride);
                                    itemToInvalidate.setAttributesName(attributes_override);
                                }
                                logger.info("Invalidating Item: " + itemToInvalidate);
                                if (!testmode) {
                                    try {
                                        client.updateItem(itemToInvalidate);
                                    } catch (ApiException e) {
                                        logger.error("ERROR trying to invalidate item ["+itemToInvalidate.getId()+"] :"+e.toString());
                                        out.println( logged_item_id+"|"+logged_item_baseurl); // keep as we have an error
                                    }
                                } else {
                                    logger.info("TESTMODE skipping Invalidate update");
                                }
                            } else {
                                out.println( logged_item_id+"|"+logged_item_baseurl);
                            }
                    }
                }
                out.close();
	        }
	    }
	}
	
	public static List<AttributeDetail> getAttributeDetailList(String confFile) throws Exception {
		List<AttributeDetail> retVal = null;

		ObjectMapper mapper = new ObjectMapper();
		AttributeDetailList attributeDetailList = mapper.readValue(new File(confFile), AttributeDetailList.class);
		if (attributeDetailList!=null) {
			retVal = attributeDetailList.attribute_detail_list;
		}
		
		return retVal ;
	}
	
	private static String readFileAsString(String fpath) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader(new File(fpath)));
		char[] buff = new char[1024];
		int numRead = 0;
		while ((numRead=reader.read(buff)) != -1) {
			String readData = String.valueOf(buff, 0, numRead);
			sb.append( readData );
		}
		return sb.toString();
	}
	
	private static void test_url_and_exit(UrlFetcher urlFetcher, String url) {

		System.out.println("-------- Attempting Extraction --------");
		System.out.println("url: "+url);
		System.out.println("---------------------------------------");
		Map<String, String> extracted_attributes = getAttributes(urlFetcher,url, "");
		if (extracted_attributes!=null) {
			Set<String> keys = extracted_attributes.keySet();
			for (String key: keys) {
				String s = String.format("%s: %s", key, extracted_attributes.get(key));
				System.out.println(s);
			}
		} else {
			System.out.println("Extraction failed!");
		}
		System.out.println("---------------------------------------");
		System.exit(0);
	}
	
}
