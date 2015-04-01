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

package io.seldon.general;

import java.util.Collection;
import java.util.Map;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.general.exception.UnknownUserAttributeException;

/**
 * Peer for {@link UserAttribute} and {@link UserAttributeValueVo} models.
 * <p/>
 * Created by: marc on 08/08/2011 at 15:42
 */
public interface UserAttributePeer {

    /**
     * Retrieve a list of all known user attributes.
     *
     * @return a collection of {@link UserAttribute user attribute}s.
     */
    public Collection<UserAttribute> getAll();

    /**
     * Persist the supplied attribute.
     *
     * @param userAttribute the attribute to persist
     * @return flag indicating whether attribute addition was successful
     */
    public Boolean addAttribute(UserAttribute userAttribute);

    /**
     * Create an attribute from the supplied name and type; persist it.
     *
     * @param attributeName name of the attribute
     * @param attributeType type name of the attribute
     * @return flag indicating whether attribute addition was successful
     */
    public Boolean addAttribute(String attributeName, String attributeType);

    /**
     * Remove a user attribute declaration.
     *
     * @param attribute the user attribute to remove
     * @throws io.seldon.db.jdo.DatabaseException from {@link io.seldon.db.jdo.TransactionPeer#runTransaction(io.seldon.db.jdo.Transaction)}
     */
    public void delete(UserAttribute attribute) throws DatabaseException;

    /**
     * Retrieves both scalar and composite attribute-value information.
     *
     * @param user the {@link User} to retrieve attribute values for.
     * @return a {@link Map} from attribute IDs ({@link Integer}s) to {@link UserAttributeValueVo} instances.
     *         (These instances encapsulate the attribute ID, value and type information).
     */
    public Map<Integer, UserAttributeValueVo> getAttributesForUser(User user);

    /**
     * Retrieves both scalar and composite attribute-value information.
     *
     * @param userId the {@link User} ID to retrieve attribute values for.
     * @return a {@link Map} from attribute IDs ({@link Integer}s) to {@link UserAttributeValueVo} instances.
     *         (These instances encapsulate the attribute ID, value and type information).
     */
    public Map<Integer, UserAttributeValueVo> getAttributesForUser(Long userId);

    /**
     * Retrieves composite attribute-value information.
     *
     * @param user the {@link User} to retrieve attribute values for.
     * @return a {@link Map} from {@link Integer} attribute IDs to {@link Integer} value IDs.
     */
    public Map<Integer, Integer> getCompositeAttributesForUser(User user);

    /**
     * Retrieves composite attribute-value information.
     *
     * @param userId the {@link User} ID to retrieve attribute values for.
     * @return a {@link Map} from {@link Integer} attribute IDs to {@link Integer} value IDs.
     */
    public Map<Integer, Integer> getCompositeAttributesForUser(Long userId);

    /**
     * Retrieves composite attribute-value name information.
     *
     * @param user the {@link User} to retrieve attribute values for.
     * @return a {@link Map} from {@link String} attribute names to {@link String} value names.
     */
    public Map<String, String> getCompositeAttributeNamesForUser(User user);

    /**
     * Retrieves composite attribute-value name information.
     *
     * @param userId the {@link User} ID to retrieve attribute values for.
     * @return a {@link Map} from {@link String} attribute names to {@link String} value names.
     */
    public Map<String, String> getCompositeAttributeNamesForUser(Long userId);

    // ~~ Individual attribute retrieval ~~~~~~~~

    public UserAttributeValueVo getScalarUserAttributeValueForUser(Long userId, Integer attributeId) throws UnknownUserAttributeException;

    public UserAttributeValueVo getScalarUserAttributeValueForUser(User user, Integer attributeId) throws UnknownUserAttributeException;

    public UserAttributeValueVo getScalarUserAttributeValueForUser(Long userId, String attributeName) throws UnknownUserAttributeException;

    public UserAttributeValueVo getScalarUserAttributeValueForUser(User user, String attributeName) throws UnknownUserAttributeException;

    public UserAttribute findUserAttributeByName(String name);

    public UserAttribute findByNameAndType(String name, Integer type);

    // ~~ Persisting attribute values ~~~~~~~~

    public void saveUserAttributeValue(UserAttributeValueVo<?> userAttributeValue);

    public void addUserAttributeValue(Long userId, Integer attributeId, Object value, String type);
    
    /**
     * Add an attribute for a user checking if the attribute is already defined in the service.
     * if not it will add the attribute to the service definition.
     * It will also check if the attribute is already defined (avoiding duplicates).
     * It also avoid adding NULL values and truncates VARCHAR values to the max allowed.
     *
     */
    public void addUserSafeAttributeValue(Long userId,String attribute, Object value, String type, Integer linkType, Boolean demographic);
    
    
	public Map<Integer,Integer> getUserAttributes(long userId);
	public Map<String,String> getUserAttributesName(long userid);

}
