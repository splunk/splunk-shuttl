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
package com.splunk.shuttl.archiver.util;

import java.util.HashMap;
import java.util.Map;

import com.splunk.shuttl.archiver.archive.SplunkEnrivonmentNotSetException;

/**
 * Methods for verifying that Splunk's environment is set.
 */
public class SplunkEnvironment {

    /**
     * @return the value of the environmen variable 'SPLUNK_HOME'
     */
    public static String getSplunkHome() {
	throwExceptionIfSplunkHomeIsNotSet();
	return getSplunkHomeFromSystem();
    }

    private static String getSplunkHomeFromSystem() {
	return System.getenv("SPLUNK_HOME");
    }

    private static void throwExceptionIfSplunkHomeIsNotSet() {
	if (!isSplunkHomeSet()) {
	    throw new SplunkEnrivonmentNotSetException();
	}
    }

    private static boolean isSplunkHomeSet() {
	return getSplunkHomeFromSystem() != null;
    }

    /**
     * @return environment variables needed to run Splunk executables.
     */
    public static Map<String, String> getEnvironment() {
	throwExceptionIfSplunkHomeIsNotSet();
	Map<String, String> environmentVars = new HashMap<String, String>();
	environmentVars.put("SPLUNK_HOME", SplunkEnvironment.getSplunkHome());
	return environmentVars;
    }

}
