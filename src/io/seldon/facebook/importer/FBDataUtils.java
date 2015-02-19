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

package io.seldon.facebook.importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.restfb.types.User;
import io.seldon.facebook.FBConstants;

public class FBDataUtils {

	public static Map<String, String> buildDemographicsFromUser(User user) {
		Map<String, String> demographics = new HashMap<>();
		
		String fbGender = fbGetGender( user.getGender() );
		if (fbGender!=null) {
			demographics.put(FBConstants.FB_GENDER, fbGender);
		}
		
		String fbBirthday = fbGetBirthday( user.getBirthday() );
		if (fbBirthday!=null) {
			demographics.put(FBConstants.FB_BIRTHDAY, fbBirthday);
		}
		
		String fbDateOfBirth = fbGetDateOfBirth( user.getBirthday() );
		if (fbDateOfBirth!=null) {
			demographics.put(FBConstants.FB_DATE_OF_BIRTH, fbDateOfBirth);
		}
		
		return demographics;
	}
	
	private static String fbGetGender(String raw_gender) {
		return raw_gender;
	}
	
	private static String fbGetBirthday(String raw_birthday) {
		String retVal = null;

		if (raw_birthday!=null) {
			retVal = raw_birthday.substring(0, 5);
		}
		
		return retVal;
	}
	
	private static String fbGetDateOfBirth(String raw_birthday) {
		String retVal = null;

		if (raw_birthday!=null && raw_birthday.length()==10) {
			
			SimpleDateFormat sf = new SimpleDateFormat("MM/dd/yyyy");
			Date d = null;
			try {
				d = sf.parse(raw_birthday);
			} catch (ParseException e) {
			}
			
			if (d!=null) {
				SimpleDateFormat sf2 = new SimpleDateFormat("yyyy-MM-dd");
				retVal = sf2.format(d);
			}
		}
		
		return retVal;
	}
}
