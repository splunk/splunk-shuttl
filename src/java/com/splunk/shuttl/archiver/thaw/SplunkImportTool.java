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

import java.util.Map;

/**
 * Class to represent the import tool in Splunk's bin directory.
 */
public class SplunkImportTool {

    /**
     * @param splunkHome
     */
    public SplunkImportTool(String splunkHome) {
	// TODO Auto-generated constructor stub
    }

    /**
     * @return command for executing Splunk import tool.
     */
    public String getExecutableCommand() {
	throw new UnsupportedOperationException();
    }

    /**
     * @return the environment needed to run the command.
     */
    public Map<String, String> getEnvironment() {
	throw new UnsupportedOperationException();
    }

    /**
     * @return
     */
    public static SplunkImportTool create() {
	return null;
    }

}
