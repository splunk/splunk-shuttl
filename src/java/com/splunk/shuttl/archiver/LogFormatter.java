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
package com.splunk.shuttl.archiver;

/**
 * A log formatter.
 * 
 */
public class LogFormatter {

    /**
     * Format: {@code happened="$message"}<br/>
     * Use when logging events not fit for will/done, such as events originated
     * from another process.
     * 
     * @return A formatted message.
     */
    public static String happened(Object message, Object... keyAndValues) {
	return combineAdditionalKeyValues(keyAndValues, "happened", message);
    }

    /**
     * Format: {@code will="$message"}<br/>
     * Use this before doing a time consuming action like an async call.
     * 
     * @return A formatted message.
     */
    public static String will(Object message, Object... keyAndValues) {
	return combineAdditionalKeyValues(keyAndValues, "will", message);
    }

    /**
     * Format: {@code done="$message"}<br/>
     * Use this after completing a task.
     * 
     * @return A formatted message.
     */
    public static String done(Object message, Object... keyAndValues) {
	return combineAdditionalKeyValues(keyAndValues, "done", message);
    }

    /**
     * Format: {@code did="$did" happened="$happened" expected="$expected"}<br/>
     * Use this when an error occurs.
     * 
     * @return A formatted message.
     */
    public static String did(Object did, Object happened, Object expected,
	    Object... keyAndValues) {
	if (expected != null)
	    return combineAdditionalKeyValues(keyAndValues, "did", did,
		    "happened", happened, "expected", expected);
	else
	    return combineAdditionalKeyValues(keyAndValues, "did", did,
		    "happened", happened);
    }

    /**
     * Format: {@code did="$did" happened="$happened" result="$result"}<br/>
     * Use this when a warning occurs.
     * 
     * @return A formatted message.
     */
    public static String warn(Object did, Object happened, Object result,
	    Object... keyValues) {
	return combineAdditionalKeyValues(keyValues, "did", did, "happened",
		happened, "result", result);
    }

    private static String combineAdditionalKeyValues(
	    Object[] additionalKeyValues, Object... logSpecificKeyValues) {
	Object[] allKeyValues = new Object[additionalKeyValues.length
		+ logSpecificKeyValues.length];
	System.arraycopy(logSpecificKeyValues, 0, allKeyValues, 0,
		logSpecificKeyValues.length);
	System.arraycopy(additionalKeyValues, 0, allKeyValues,
		logSpecificKeyValues.length, additionalKeyValues.length);
	return pairKeysWithValues(allKeyValues);
    }

    private static String pairKeysWithValues(Object... messages) {
	StringBuffer sb = new StringBuffer();
	try {
	    sb.append(messages[0]);
	    sb.append("=\"");
	    sb.append(messages[1]);
	    sb.append("\"");
	    for (int i = 2; i < messages.length; i += 2) {
		sb.append(" ");
		sb.append(messages[i]);
		sb.append("=\"");
		sb.append(messages[i + 1]);
		sb.append("\"");
	    }
	} catch (ArrayIndexOutOfBoundsException e) {
	    throw new IllegalArgumentException(
		    "You need even number of arguments to create key/value pairs");
	}
	return sb.toString();
    }

}
