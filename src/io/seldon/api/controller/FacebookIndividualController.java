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

package io.seldon.api.controller;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.general.ActionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.exception.FacebookOAuthException;
import com.restfb.types.User;
import io.seldon.api.logging.FacebookLogger;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.ActionService;
import io.seldon.api.resource.service.UserService;
import io.seldon.facebook.RummbleFacebookAccountsPeer;
import io.seldon.facebook.Like;
import io.seldon.facebook.RummbleFacebookAccount;
import io.seldon.general.jdo.SqlActionPeer;

/**
 * @author alex
 */

//@Controller
public class FacebookIndividualController {

	@Autowired
	ActionService actionService;
	  
	
	@RequestMapping(value="/fbaccount", method = RequestMethod.GET)
	public @ResponseBody
	ResourceBean retrieveDimensions(HttpServletRequest req) {

		ConsumerBean cb = new ConsumerBean();
		cb.setShort_name("facebook");

		SqlActionPeer actionPeer = Util.getActionPeer(cb);

		// use action_type "fblike"
		ActionType at = actionPeer.getActionType("fblike");
		if(at == null) {
			// create new "fblike" ActionType
			at = new ActionType(null, "fblike", new Double(1), null,null,null);	
		}

		RummbleFacebookAccountsPeer accounts = new RummbleFacebookAccountsPeer();

		//		Collection<RummbleFacebookAccount> c = accounts.getRummbleFacebookAccounts(100);

		RummbleFacebookAccount account = accounts.getFacebookAccount(new Long(160879));


		Date start = new Date();
		System.out.println(account.getId());

		try {

			final FacebookClient facebookClient = new DefaultFacebookClient(account.getoAuthToken());

			// It's also possible to create a client that can only access
			// publicly-visible data - no access token required. 
			// FacebookClient publicOnlyFacebookClient = new DefaultFacebookClient();

			User fbUser = facebookClient.fetchObject("me", User.class);

			System.out.println("User name: " + fbUser.getName());

			UserBean user = new UserBean();
			// check if user is currently in the database
			try {

				Long userId = UserService.getInternalUserId(cb, fbUser.getId());
				user = UserService.getUser(cb, fbUser.getId(), true);

			} catch(APIException ex) {

				user = new UserBean(fbUser.getId());
				UserService.addUser(cb, user);

			}

			String fbConn = "me/likes";
			boolean firstPage = true;
			Connection<Like> likes = facebookClient.fetchConnection(fbConn, Like.class, Parameter.with("limit", 100));
			int totalLikes = likes.getData().size();

			do {

				if(!firstPage) {
					likes = facebookClient.fetchConnectionPage(fbConn, Like.class);
					totalLikes += likes.getData().size();
				}

				// create new item with item_attr name (in item table), category (enum) and createdDate (goes into action)
				// SqlItemPeer itemPeer = Util.getItemPeer(cb);

				ItemBean item = new ItemBean();

				for(Like like : likes.getData()) {

					// see if item currently exists, if not create it.
					try {

						Long itemId = (ItemService.getInternalItemId(cb, like.getId()));
						item = ItemService.getItem(cb, like.getId(), true);

					} catch(APIException ex) {

						String name = like.getName();
						if (name != null && name.length() > 250)
							name = name.substring(0, 250);
						item = new ItemBean(like.getId(), name);

						Map<String, String> attr = new HashMap<String, String>();
						attr.put("category", like.getCategory());

						item.setAttributesName(attr);

						ItemService.addItem(cb, item);

					}

					// ListBean userActions = ActionService.getUserActions(cb, user.getId(), 0, true);
					ListBean userActions = ActionService.getActions(cb, user.getId(), item.getId(), 0, true);
					// TODO: test if the "fblike" action has occured?

					if(userActions.getSize() < 1) {

						ActionBean actionBean = new ActionBean(null, user.getId(), item.getId(), at.getTypeId(), like.getCreatedTime(), null, 1);
						actionService.addAction(cb, actionBean);

					}

				}						

				fbConn = likes.getNextPageUrl();
				firstPage = false;

			} while(likes.hasNext());


			accounts.setAccessToken(account, true);
			FacebookLogger.log(start, new Date(), user.getId(), totalLikes, FacebookLogger.Result.SUCCESS);


		} catch(FacebookOAuthException e) {

			accounts.setAccessToken(account, false);
			FacebookLogger.log(start, new Date(), String.valueOf(account.getId()), 0, FacebookLogger.Result.ERROR);

		}

		return null;
	}

}
