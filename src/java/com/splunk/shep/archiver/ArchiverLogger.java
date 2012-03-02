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
 * A log class until the proper mecanism is in place.
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
    public static void will(String message, String... keyAndValues) {
	combineAdditionalKeyValuesAndLog(keyAndValues, "will", message);
    }

    /**
     * Logs: done="$message" Use this after completing a task.
     */
    public static void done(String message, String... keyAndValues) {
	combineAdditionalKeyValuesAndLog(keyAndValues, "done", message);
    }

    /**
     * Logs: did="$did" happened="$happened" expected="$expected" Use this when
     * an error occurs.
     */
    public static void did(String did, String happened, String expected,
	    String... keyAndValues) {
	combineAdditionalKeyValuesAndLog(keyAndValues, "did", did, "happened",
		happened, "expected", expected);
    }

    private static void combineAdditionalKeyValuesAndLog(
	    String[] additionalKeyValues, String... logSpecificKeyValues) {
	String[] allKeyValues = new String[additionalKeyValues.length
		+ logSpecificKeyValues.length];
	System.arraycopy(logSpecificKeyValues, 0, allKeyValues, 0,
		logSpecificKeyValues.length);
	System.arraycopy(additionalKeyValues, 0, allKeyValues,
		logSpecificKeyValues.length, additionalKeyValues.length);
	log(pairKeysWithValues(allKeyValues));
    }

    private static String pairKeysWithValues(String... strings) {
	StringBuffer sb = new StringBuffer();
	try {
	    sb.append(strings[0]);
	    sb.append("=\"");
	    sb.append(strings[1]);
	    sb.append("\"");
	    for (int i = 2; i < strings.length; i += 2) {
		sb.append(" ");
		sb.append(strings[i]);
		sb.append("=\"");
		sb.append(strings[i + 1]);
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

}
