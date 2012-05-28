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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.util.SplunkEnvironment;

/**
 * Calls Splunk's exporttool for exporting a bucket to a new format. I.e. csv
 */
public class SplunkExportTool {

    private static final Logger logger = Logger
	    .getLogger(SplunkExportTool.class);

    /**
     * @return command to execute the export tool.
     */
    public String getExecutableCommand() {
	if (SplunkEnvironment.isSplunkHomeSet()) {
	    return getPathToExecutableExportTool();
	} else {
	    logWarningAboutNotExportingTheBucket();
	    throw new SplunkEnrivonmentNotSetException();
	}
    }

    private String getPathToExecutableExportTool() {
	return new File(SplunkEnvironment.getSplunkHome(), "/bin/exporttool")
		.getAbsolutePath();
    }

    private void logWarningAboutNotExportingTheBucket() {
	logger.debug(warn("Getting the exporttool from Splunk home.",
		"$SPLUNK_HOME was not set", "Throwing exception"));
    }

    /**
     * @return environment variables to run export tool.
     */
    public Map<String, String> getEnvironmentVariables() {
	if (SplunkEnvironment.isSplunkHomeSet()) {
	    return SplunkEnvironment.getEnvironment();
	} else {
	    throw new SplunkEnrivonmentNotSetException();
	}
    }

}
