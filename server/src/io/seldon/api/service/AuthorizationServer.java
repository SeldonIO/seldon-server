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

package io.seldon.api.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import io.seldon.api.resource.ScopedConsumerBean;
import io.seldon.api.APIException;
import io.seldon.api.jdo.TokenPeer;
import io.seldon.api.resource.TokenBean;
import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.NewClientListener;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import io.seldon.api.Constants;
import io.seldon.api.jdo.Consumer;
import io.seldon.api.jdo.ConsumerPeer;
import io.seldon.api.jdo.Token;

/**
 * @author claudio
 */

@Service
public class AuthorizationServer implements NewClientListener {
    private final static Pattern pattern = Pattern.compile("/S+");
    private final static Logger logger = Logger.getLogger(AuthorizationServer.class);

    private final static Map<String, ScopedConsumerBean> consumerCache = new ConcurrentHashMap<>();
    public static final int CONSUMER_REFRESH_INTERVAL = 300000;

	@Autowired
	private JDOFactory jdoFactory;

	@Autowired
	private ConsumerPeer consumerPeer;

	@Autowired
	private TokenPeer tokenPeer;

	@Autowired
	private ClientConfigHandler clientConfigHandler;

	@PostConstruct
	public void startup(){
		clientConfigHandler.addNewClientListener(this, false);
	}

    //METHODS
    public ScopedConsumerBean getConsumer(HttpServletRequest request) throws APIException {
        if (request == null) {
            throw new APIException(APIException.NOT_VALID_CONNECTION);
        }
        String consumerKey = request.getParameter(Constants.CONSUMER_KEY);
        return retrieveConsumerBean(consumerKey);
    }

    private ScopedConsumerBean retrieveConsumerBean(String consumerKey) {
        return retrieveConsumerBean(consumerKey, true);
    }

    private ScopedConsumerBean retrieveConsumerBean(String consumerKey, boolean allowCached) {
        if (StringUtils.isBlank(consumerKey)) {
            throw new APIException(APIException.NOT_SPECIFIED_CONS_KEY);
        }
        final ScopedConsumerBean cachedConsumerBean = consumerCache.get(consumerKey);
        if (cachedConsumerBean != null && allowCached) {
            return cachedConsumerBean;
        }
        final Consumer consumer = consumerPeer.findConsumer(consumerKey);
        if (consumer == null || StringUtils.isNotBlank(consumer.getSecret())) {
            // As a precaution: This method is only valid for consumers set up without a secret.
            throw new APIException(APIException.NOT_AUTHORIZED_CONS);
        }
        if (!consumer.isActive()) {
            throw new APIException(APIException.NOT_AUTHORIZED_CONS);
        }
        final ScopedConsumerBean consumerBean = new ScopedConsumerBean(consumer.getShort_name(), consumer.getScope(), consumer.getUrl());
        consumerCache.put(consumerKey, consumerBean);
        return consumerBean;
    }

    @Scheduled(fixedDelay = CONSUMER_REFRESH_INTERVAL)
    public void refreshConsumerCache() {
        try {
            logger.info("Refreshing consumer cache (" + consumerCache.size() + " entries).");
            for (String consumerKey : consumerCache.keySet()) {
                try {
                    final ScopedConsumerBean consumerBean = retrieveConsumerBean(consumerKey, false);
                    consumerCache.put(consumerKey, consumerBean);
                } catch (Exception e) {
                    logger.warn("Problem refreshing consumer cache entry for: " + consumerKey, e);
                    consumerCache.remove(consumerKey);
                }
            }
        } finally {
			jdoFactory.cleanupPM();
        }
    }

	public Token getToken(HttpServletRequest req) throws APIException {
		return getToken(req,true);
	}
	/**
	 * @return token
	 * Create and return an access token for a specific consumer.
	 * It generates a new token even if the consumer has already a valid token active
	 */
	public Token getToken(HttpServletRequest req,boolean makeTransient) throws APIException {
		
		//init
		String consumerKey = null;
		String consumerSecret = null;
		Token token = null;
		//if request is null
		if(req == null) {
			throw new APIException(APIException.NOT_VALID_CONNECTION);
		}
		//retrieve from the request the consumer key and the consumer secret
		consumerKey = req.getParameter(Constants.CONSUMER_KEY);
		consumerSecret = req.getParameter(Constants.CONSUMER_SECRET);
		//check if the consumerId is set
		if(consumerKey == null || consumerKey.trim().equals("")) {
			throw new APIException(APIException.NOT_SPECIFIED_CONS_KEY);
		}
		//check if the consumerSecret is set
		if(consumerSecret == null || consumerSecret.trim().equals("")) {
			throw new APIException(APIException.NOT_SPECIFIED_CONS_SECRET);
		}
		//check if the consumer credentials are valid
		Consumer consumer = isConsumerValid(consumerKey,consumerSecret);
		//check if the consumer is secure and request is TLS
		if(consumer.isSecure() && !req.isSecure()) {
			throw new APIException(APIException.NOT_SSL_CONN);
		}
		token = new Token(consumer);
		//make the token persistent
		tokenPeer.saveToken(token);
		if (makeTransient)
		{
			//RAS-34 (ensure token is transient so JDO doesn't try to refresh against the read-replica the fields
			//Maybe a better solution?
			jdoFactory.getPersistenceManager(Constants.API_DB).makeTransient(token);
		}
		return token;
	}
	
	/**
	 * @param consumerId
	 * @param consumerSecret
	 * @return boolean
	 * Check if the pair consumerId/consumerSecret is valid
	 */
	public Consumer isConsumerValid(String consumerId, String consumerSecret)  throws APIException {
		Consumer consumer = null;
		if(consumerId == null || consumerId.trim().equals("") ||consumerSecret == null || consumerSecret.trim().equals("")) {
			throw new APIException(APIException.NOT_AUTHORIZED_CONS);
		}
		//if consumer key does not exists
		consumer = consumerPeer.findConsumer(consumerId);
		if(consumer == null) {
			throw new APIException(APIException.NOT_VALID_KEY_CONS);
		}
		//if consumer secret is not valid
		if(!consumer.getSecret().trim().equals(consumerSecret.trim())) {
			throw new APIException(APIException.NOT_VALID_SECRET_CONS);
		}
		//if consumer is not active
		if(!consumer.isActive()) {
			throw new APIException(APIException.NOT_AUTHORIZED_CONS);
		}
		return consumer;
	}
	
	public TokenBean isTokenValid(HttpServletRequest req) throws APIException {
		//init
		String tokenKey = null;
		boolean safe = true;
		
		//if request is null
		if(req == null) {
			throw new APIException(APIException.NOT_VALID_CONNECTION);
		}
		//retrieve from the request the consumer key and the consumer secret
		//from heder
		String authorization = req.getHeader(Constants.AUTHORIZATION);
		if(authorization!=null && !authorization.trim().equals("")) {
			Matcher matcher = pattern.matcher(authorization);
			if(matcher.find()) {
				tokenKey = matcher.group(1);
			}
		}
		//try to retrieve the token from the parameters
		if(tokenKey == null || tokenKey.trim().equals("")) {
			tokenKey = req.getParameter(Constants.OAUTH_TOKEN);
			//token not sent in a safe way
			safe = false;
		}
		//TODO
		//check presence of token in the body

		//if tokenKey empty
		if(tokenKey == null || tokenKey.trim().equals("")) {
			throw new APIException(APIException.NOT_SPECIFIED_TOKEN);
		}
		
		//check if token is in memcached
		//if is in memcached replicate the expired and security controls?
		TokenBean res = (TokenBean) MemCachePeer.get(MemCacheKeys.getTokenBeanKey(tokenKey));
		if(res==null) {
			Token t = tokenPeer.findToken(tokenKey);
			//if token not existing
			if(t == null) {
				throw new APIException(APIException.NOT_VALID_TOKEN_KEY);
			}
			
			if(t.getConsumer().isSecure() && !safe) {
				throw new APIException(APIException.NOT_SECURE_TOKEN);
			}
			//if token expired or no longer valid
			if(tokenPeer.isExpired(t)) {
				throw new APIException(APIException.NOT_VALID_TOKEN_EXPIRED);
			}
			res = new TokenBean(t);
		}
		return res;
	}

	@Override
	public void clientAdded(String client, Map<String, String> initialConfig) {
		logger.info("Reloading consumer table due to new client " + client);
		refreshConsumerCache();
	}

	@Override
	public void clientDeleted(String client) {
		// ignore
	}
}
