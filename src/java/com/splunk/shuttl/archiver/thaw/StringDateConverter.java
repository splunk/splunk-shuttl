// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver.thaw;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class for converting strings to {@link Date}
 */
public class StringDateConverter {

    /**
     * @return
     */
    public static Date convert(String dateAsString) {
	Date date = tryLongDate(dateAsString);
	if (date != null)
	    return date;
	else
	    return parseString(dateAsString);
    }

    private static Date tryLongDate(String dateAsString) {
	try {
	    return new Date(Long.parseLong(dateAsString));
	} catch (NumberFormatException e) {
	    return null;
	}
    }

    private static Date parseString(String dateAsString) {
	try {
	    return new SimpleDateFormat("yyyy-MM-dd").parse(dateAsString);
	} catch (ParseException e) {
	    return null;
	}
    }

}
