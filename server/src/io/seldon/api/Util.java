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

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ListBean;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.general.UserAttributePeer;
import io.seldon.general.jdo.SqlActionPeer;
import io.seldon.general.jdo.SqlItemPeer;
import io.seldon.general.jdo.SqlUserAttributePeer;
import io.seldon.general.jdo.SqlUserPeer;
import io.seldon.general.jdo.SqlVersionPeer;
import io.seldon.recommendation.CFAlgorithm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.servlet.http.HttpServletRequest;


public class Util {

    private static AlgorithmService algorithmService;
 
	public static void setAlgorithmService(AlgorithmService algorithmService) {
        Util.algorithmService = algorithmService;
    }

    public static AlgorithmService getAlgorithmService() {
        return algorithmService;
    }
	
	
	
	public static SqlItemPeer getItemPeer(String client) throws APIException {
		PersistenceManager pm = JDOFactory.get().getPersistenceManager(client);
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlItemPeer(pm);
	}
	
	public static SqlItemPeer getItemPeer(ConsumerBean c) throws APIException {
		PersistenceManager pm = JDOFactory.get().getPersistenceManager(c.getShort_name());
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlItemPeer(pm);
	}
	
	public static SqlItemPeer getItemPeer(PersistenceManager pm) throws APIException {
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlItemPeer(pm);
	}

    public static SqlUserPeer getUserPeer(ConsumerBean c) throws APIException {
        return getUserPeer(c.getShort_name());
    }
	
	public static SqlUserPeer getUserPeer(String consumer) throws APIException {
		PersistenceManager pm = JDOFactory.get().getPersistenceManager(consumer);
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlUserPeer(pm);
	}
	
	public static SqlUserPeer getUserPeer(PersistenceManager pm) throws APIException {
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlUserPeer(pm);
	}
	

	
	public static SqlActionPeer getActionPeer(ConsumerBean c) throws APIException {
		PersistenceManager pm = JDOFactory.get().getPersistenceManager(c.getShort_name());
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlActionPeer(pm);
	}
	
	public static SqlActionPeer getActionPeer(PersistenceManager pm) throws APIException {
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlActionPeer(pm);
	}
	
	
	

    public static SqlVersionPeer getVersionPeer(ConsumerBean c) throws APIException {
        PersistenceManager pm = JDOFactory.get().getPersistenceManager(c.getShort_name());
        return getVersionPeer(pm);
    }

    private static SqlVersionPeer getVersionPeer(PersistenceManager pm) {
        if (pm == null) {
            throw new APIException(APIException.INTERNAL_DB_ERROR);
        }
        return new SqlVersionPeer(pm);
    }

    public static int getLimit(HttpServletRequest req) throws NumberFormatException {
		int limit = Constants.DEFAULT_RESULT_LIMIT;
		String sLimit = req.getParameter(Constants.URL_LIMIT);
		if(sLimit != null) {
			limit = Integer.parseInt(sLimit);
		}
		return limit;
	}
	

	public static boolean getFull(HttpServletRequest req) {
		boolean full = false;
		String sFull = req.getParameter(Constants.URL_FULL);
		if(sFull != null && sFull.equals(Boolean.TRUE.toString())) {
			full = Boolean.TRUE;
		}
		return full;
	}
	
	public static String getName(HttpServletRequest req) {
		return req.getParameter(Constants.URL_NAME);
	}
	
	
	public static List<String> getKeywords(HttpServletRequest req) {
		List<String> keywords = null;
		String sKeywords = req.getParameter(Constants.URL_KEYWORD);
		if(sKeywords != null) {
			keywords = Arrays.asList(sKeywords.split(","));
		}
		return keywords;
	}

	public static Integer getDimension(HttpServletRequest req) throws NumberFormatException {
		Integer dimension = 0;
		String sDim = req.getParameter(Constants.URL_ATTR_DIMENSION);
		if(sDim != null) {
			dimension = Integer.parseInt(sDim);
		}
		return dimension;
	}
	
	public static Set<Integer> getDimensions(HttpServletRequest req) throws NumberFormatException {
		Integer dimension = 0;
		String sDim = req.getParameter(Constants.URL_ATTR_DIMENSIONS);
		if(sDim != null) {
			String[] dParts = sDim.split(",");
			Set<Integer> dimensions = new HashSet<Integer>();
			for(int i=0;i<dParts.length;i++)
				dimensions.add(Integer.parseInt(dParts[i]));
			return dimensions;
		}
		else
			return new HashSet<Integer>();
	}
	
	public static ListBean getLimitedBean(ListBean bean, int limit) {
		if(bean != null && bean.getSize() == limit) { 
			bean.setRequested(limit); 
		}
		else if(bean != null && bean.getSize() < bean.getRequested() && bean.getSize() <= limit) 
		{ 
			bean.setRequested(limit);
		}
		else if(bean != null && bean.getSize() > limit) 
		{ 
			bean.setRequested(limit); 
			bean.setSize(limit); 
			bean.setList(bean.getList().subList(0,limit)); 
		}
		else bean = null;
		return bean;
	}

	public static String getSort(HttpServletRequest req) {
		return req.getParameter(Constants.URL_SORT);
	}

	public static UserAttributePeer getUserAttributePeer(ConsumerBean c) throws APIException {
		PersistenceManager pm = JDOFactory.get().getPersistenceManager(c.getShort_name());
		return getUserAttributePeer(pm);
	}
	
	public static UserAttributePeer getUserAttributePeer(PersistenceManager pm) throws APIException {
		if(pm == null) {
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
		return new SqlUserAttributePeer(pm);
	}

	public static Integer getType(HttpServletRequest req) {
		Integer res = null;
		String s = req.getParameter(Constants.URL_TYPE);
		if(s!=null && s.length()>0) {
			try {
				res = Integer.parseInt(s);
			}
			catch(NumberFormatException e) { }
		}
		return res;
	}
	
	 public static final String algOptionSeparator = ",";
	 private static final String parameterSeparator = ":";
	 private static final String valueSeparator = "|";
	 
	public static List<String> getAlgorithms(HttpServletRequest req) {
		List<String> res = null;
		String algorithms = req.getParameter(Constants.URL_ALGORITHMS);
		if(algorithms != null && !algorithms.isEmpty()) {
			res  = Arrays.asList(algorithms.split(algOptionSeparator));
		}
		return res;
	}
	
	
	
	public static CFAlgorithm getAlgorithmOptions(ConsumerBean c, List<String> algorithms,String recTag) throws CloneNotSupportedException {
		CFAlgorithm cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(c,recTag).clone();
		for(String algorithm : algorithms) {
			try {
				if(!algorithm.isEmpty() && algorithm.contains(parameterSeparator)) {
					String[] parameter = algorithm.split("\\"+parameterSeparator);
					String field = parameter[0];
					List<String> values = Arrays.asList(parameter[1].split("\\"+valueSeparator));
					cfAlgorithm.setParameter(field,values);
				}
			}
			catch(Exception e) {}
		}
		
		return cfAlgorithm;
	}
}
