/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.api;

/**
 * @author claudio
 */

public class Constants {
	public final static String CONTENT_TYPE_JSON = "application/json";
	public final static String CACHE_CONTROL = "Cache-Control";
	public final static String NO_CACHE = "no-store";
	public final static String ACCESS_TOKEN = "access_token";
	public final static String TOKEN_TYPE = "token_type";
	public final static String TOKEN_SCOPE = "token_scope";
	public final static String EXPIRES_IN = "expires_in";
	public final static String GET = "GET";
	public final static String POST = "POST";
	public final static String PUT = "PUT";
	public final static String DELETE = "DELETE";
	public final static String AUTHORIZATION = "Authorization";
	public final static String API_DB = "api";
	public final static long TOKEN_TIMEOUT = 6000;
	public final static String CONSUMER_KEY = "consumer_key";
	public final static String CONSUMER_SECRET = "consumer_secret";
	public final static String OAUTH_TOKEN = "oauth_token";
	public final static String TOKEN_ALL_SCOPE = "all";
	public final static String TOKEN_READONLY_SCOPE = "readonly";
    public static final String TOKEN_JAVASCRIPT_SCOPE = "js";

	//RESOURCE NAME
	public final static String DEFAULT_RESOURCE_NAME = "resource";
	public final static String TOKEN_RESOURCE_NAME = "token";
	public final static String CONSUMER_RESOURCE_NAME = "consumer";
	public final static String ERROR_RESOURCE_NAME = "error";
	public final static String USER_RESOURCE_NAME = "user";
	public final static String USERS_RESOURCE_NAME = "users";
	public final static String USERTRUSTNODE_RESOURCE_NAME = "usertrustnode";
	public final static String USERTRUSTGRAPH_RESOURCE_NAME = "usertrustgraph";
	public final static String OPINION_RESOURCE_NAME = "opinion";
	public final static String OPINIONS_RESOURCE_NAME = "opinions";
	public final static String RECOMMENDATION_RESOURCE_NAME = "recommendation";
	public final static String RECOMMENDATIONS_RESOURCE_NAME = "recommendations";
	public final static String ITEM_RESOURCE_NAME = "item";
	public final static String ITEMS_RESOURCE_NAME = "items";
	public final static String ITEMSIMILARITYNODE_RESOURCE_NAME = "itemsimilaritynode";
	public final static String ITEMSIMILARITYGRAPH_RESOURCE_NAME = "itemsimilaritygraph";
	//DEFAULT VALUES
	public final static int DEFAULT_TIMES = 1;
	public final static int DEFAULT_DIMENSION = 0;
	public final static int DEFAULT_DEMOGRAPHIC = 0;
	public final static int DEFAULT_RESULT_LIMIT = 10;
	public final static int DEFAULT_BIGRESULT_LIMIT = 100;
	public final static long POSITION_NOT_DEFINED = 0;
	public final static int USER_ITEM_NOINTERACTION = 0;
	public final static int USER_ITEM_DEFAULT_INTERACTION = 1;
	public final static int OPINION_NOT_DEFINED_VALUE = -1;
	public final static double SIMILARITY_NOT_DEFINED = -1.0;
	public final static double TRUST_NOT_DEFINED = -1.0;
	public final static int CACHING_TIME = 3600;
	public final static int USERBEAN_CACHING_TIME = 7200;
	public final static int LONG_CACHING_TIME = 86400;
	public static boolean CACHING = true;
	public final static double DEFAULT_OPINION_VALUE = 0;
	public final static int NO_LIMIT = Integer.MAX_VALUE;
	//URL QUERY PARAMETER
	public final static String URL_LIMIT = "limit";
	public final static String URL_FULL = "full";
	public final static String URL_KEYWORD = "keyword";
	public static final int NO_TRUST_DIMENSION = -1;
	public static final String URL_ATTR_DIMENSION = "dimension";
	public static final String URL_NAME = "name";
	public static final String URL_SORT = "sort";
	public static final String URL_TYPE = "type";
	public static final String URL_LINK_TYPE = "linktype";
	public static final String URL_ALGORITHMS = "algorithms";
	//SORT FIELDs
	public final static String SORT_ID = "id";
	public final static String SORT_NAME = "name";
	public final static String SORT_LAST_ACTION = "last_action";
	public final static String SORT_POPULARITY = "popularity";
	public final static String SORT_DATE = "date";
	
	//USER
	public final static int DEFAULT_USER_TYPE = 1;
	
	//ITEM
	public final static int DEFAULT_ITEM_TYPE= 1;
	public final static int ITEM_NOT_VALID = 0;
	
	//TYPES
	public final static String TYPE_VARCHAR = "VARCHAR";
	public final static String TYPE_INT = "INT";
	public final static String TYPE_BIGINT = "BIGINT";
	public final static String TYPE_DOUBLE = "DOUBLE";
	public final static String TYPE_TEXT = "TEXT";
	public final static String TYPE_DATETIME = "DATETIME";
	public final static String TYPE_BOOLEAN = "BOOLEAN";
	public final static String TYPE_ENUM = "ENUM";
	
	//attribute id defining the item type
	public final static int ATTRIBUTE_TYPE = 0;
	public static final int VARCHAR_SIZE = 255;
	public static final int DEFAULT_LINK_TYPE = 1;
	
	//Default item attributes
	public final static String ITEM_ATTR_TITLE = "title";
	public final static String ITEM_ATTR_IMG = "img_url";
	public final static String ITEM_ATTR_CAT = "category";
	public final static String ITEM_ATTR_SUBCAT = "subcategory";
	public final static String ITEM_ATTR_TAGS = "tags";
	
	//LOG
	public final static int LIST_LOG_LIMIT = 10;

    // ID prefixes
    public static final String FACEBOOK_ID_PREFIX = "_fb_";

    public static final Long ANONYMOUS_USER = -1L;
    
    public static String DEFAULT_CLIENT = "default_client";
}
