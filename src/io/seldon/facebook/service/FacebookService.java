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

package io.seldon.facebook.service;

import io.seldon.api.exception.UserMissingAttributesException;
import io.seldon.api.exception.UserNotFoundException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.exception.FacebookDisabledException;
import io.seldon.facebook.exception.FacebookIdException;
import io.seldon.facebook.exception.FacebookTokenException;

/**
 * Created by: marc on 24/08/2011 at 11:24
 */
public interface FacebookService {

//    public void performAsyncImport(ConsumerBean consumerBean, UserBean userBean,
//                                   String facebookToken, String facebookId,
//                                   FacebookImporterConfiguration configuration);

    void performAsyncImport(ConsumerBean consumerBean, UserBean userBean)
            throws UserNotFoundException, UserMissingAttributesException, FacebookDisabledException, FacebookIdException, FacebookTokenException;

}
