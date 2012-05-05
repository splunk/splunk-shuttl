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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

/**
 * Calls Splunk's exporttool for exporting a bucket to a new format. I.e. csv
 */
public class SplunkExportTool {

    private static final Logger logger = Logger
	    .getLogger(SplunkExportTool.class);

    /**
     * @return the exporttool file.
     */
    public File getExecutableFile() {
	if (!isSplunkHomeEnvironmentSet()) {
	    logWarningAboutNotExportingTheBucket();
	    throw new SplunkEnrivonmentNotSetException();
	} else {
	    return new File(new File(getSplunkHome()), "/bin/exporttool");
	}
    }

    /**
     * @return true if $SPLUNK_HOME enviroment variable is set. It's needed to
     *         locate the exporttool.
     */
    private boolean isSplunkHomeEnvironmentSet() {
	return getSplunkHome() != null;
    }

    private String getSplunkHome() {
	return System.getenv("SPLUNK_HOME");
    }

    private void logWarningAboutNotExportingTheBucket() {
	logger.debug(warn("Getting the exporttool from Splunk home.",
		"$SPLUNK_HOME was not set", "Throwing exception"));
    }

}
