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

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ResourceBean;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMethod;

import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.TokenBean;

/**
 * @author claudio
 */

@Service
public class ResourceServer {
	private final static Logger logger = Logger.getLogger(ResourceServer.class);


	@Autowired
	private AuthorizationServer authorizationServer;
	/**
	 * @return Resource r
	 * @throws io.seldon.api.APIException
	 * return the resource representation
	 */
	public ResourceBean validateResourceRequest(HttpServletRequest req) {
		ResourceBean bean;
		try {
			//check if the token is valid
			TokenBean t = authorizationServer.isTokenValid(req);
            final String tokenScopeName = t.getToken_scope();
            final TokenScope.Scope tokenScope = TokenScope.fromString(tokenScopeName);

            switch (tokenScope) {
                case READONLY:
                    if (!req.getMethod().equals(RequestMethod.GET.toString())) {
                    	logger.warn("Mismatch in scope. Got "+req.getMethod()+" needed "+RequestMethod.GET.toString());
                        throw new APIException(APIException.METHOD_NOT_AUTHORIZED);
                    }
                    break;
                case ALL:
                    break;
                default:
                    // Everything else prohibited
                    throw new APIException(APIException.METHOD_NOT_AUTHORIZED);
            }
			bean = new ConsumerBean(t);
		}
		catch(APIException e) {
			bean = new ErrorBean(e);
		}
		catch(Exception e) {
			bean = new ErrorBean(new APIException(APIException.GENERIC_ERROR));
		}
		return bean;
	}
}
