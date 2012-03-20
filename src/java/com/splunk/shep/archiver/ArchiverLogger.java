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
package com.splunk.shep.archiver;

import java.io.PrintWriter;
import java.util.Date;

/**
 * A log class until the proper mechanism is in place.
 * 
 */
public class ArchiverLogger {

    /**
     * Use {@link #log(String)} instead of calling anything on this variable.
     */
    /* package private */static PrintWriter logger = new PrintWriter(
	    System.out, true);

    /**
     * Logs: will="$message" Use this before doing a time consuming action like
     * a async call.
     */
    public static void will(Object message, Object... keyAndValues) {
	combineAdditionalKeyValuesAndLog(keyAndValues, "will", message);
    }

    /**
     * Logs: done="$message" Use this after completing a task.
     */
    public static void done(Object message, Object... keyAndValues) {
	combineAdditionalKeyValuesAndLog(keyAndValues, "done", message);
    }

    /**
     * Logs: did="$did" happened="$happened" expected="$expected" Use this when
     * an error occurs.
     */
    public static void did(Object did, Object happened, Object expected,
	    Object... keyAndValues) {
	combineAdditionalKeyValuesAndLog(keyAndValues, "did", did, "happened",
		happened, "expected", expected);
    }

    private static void combineAdditionalKeyValuesAndLog(
	    Object[] additionalKeyValues, Object... logSpecificKeyValues) {
	Object[] allKeyValues = new Object[additionalKeyValues.length
		+ logSpecificKeyValues.length];
	System.arraycopy(logSpecificKeyValues, 0, allKeyValues, 0,
		logSpecificKeyValues.length);
	System.arraycopy(additionalKeyValues, 0, allKeyValues,
		logSpecificKeyValues.length, additionalKeyValues.length);
	log(pairKeysWithValues(allKeyValues));
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

    /**
     * Atomically logs the given string.
     */
    private synchronized static void log(String logString) {
	logger.printf("[%s] %s%n", new Date().toString(), logString);
    }

    /**
     * Logs a warning about what was done, what happened and the results of this
     * warning.
     */
    public static void warn(Object did, Object happened, Object result,
	    Object... keyValues) {
	combineAdditionalKeyValuesAndLog(keyValues, "WARNING: did", did,
		"happened", happened, "result", result);
    }
}
