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
package com.splunk.shuttl.archiver.importexport.csv.splunk;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The extending classes implement the {@link SplunkTool#getToolName()} to
 * become a tool which lives in splunk's bin directory.
 */
public abstract class SplunkTool {

	public abstract String getToolName();

	/**
	 * @return command for executing Splunk import tool.
	 */
	public List<String> getExecutableCommand() {
		return Arrays.asList(SplunkEnvironment.getSplunkHome() + "/bin/splunk",
				"cmd", getToolName());
	}

	/**
	 * @return the environment needed to run the command.
	 */
	public Map<String, String> getEnvironment() {
		return SplunkEnvironment.getEnvironment();
	}

}
