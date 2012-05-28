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

/**
 * Methods for verifying that Splunk's environment is set.
 */
public class SplunkEnvironment {

    /**
     * @return true if $SPLUNK_HOME enviroment variable is set. It's needed to
     *         locate the exporttool.
     */
    public static boolean isSplunkHomeSet() {
	return getSplunkHome() != null;
    }

    public static String getSplunkHome() {
	return System.getenv("SPLUNK_HOME");
    }

    /**
     * @return environment variables needed to run Splunk executables.
     */
    public static Map<String, String> getEnvironment() {
	Map<String, String> environmentVars = new HashMap<String, String>();
	environmentVars.put("SPLUNK_HOME", SplunkEnvironment.getSplunkHome());
	return environmentVars;
    }

}
