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

import com.splunk.shep.archiver.model.Bucket;

/**
 * Calls Splunk's exporttool for exporting a bucket to a new format. I.e. csv
 */
public class SplunkExportTool {

    private static final Logger logger = Logger
	    .getLogger(SplunkExportTool.class);
    private final Runtime runtime;

    public SplunkExportTool(Runtime runtime) {
	this.runtime = runtime;
    }

    /**
     * @param bucket
     *            to export to csv.
     * @return the csv file of the bucket.
     */
    public File exportToCsv(Bucket bucket) {
	if (!isSplunkHomeEnvironmentSet()) {
	    logWarningAboutNotExportingTheBucket(bucket);
	    throw new SplunkEnrivonmentNotSetException();
	} else {
	    return exportBucketToCsvWithExportTool(bucket);
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

    private void logWarningAboutNotExportingTheBucket(Bucket bucket) {
	logger.warn(warn("Attempted to call splunks export tool",
		"$SPLUNK_HOME was not set",
		"Won't export. Just returning the bucket unexported.",
		"bucket", bucket));
    }

    private File exportBucketToCsvWithExportTool(Bucket bucket) {
	String pathToExportTool = getSplunkHome() + "/bin/exporttool";
	String pathToBucket = bucket.getDirectory().getAbsolutePath();
	String csvOutput = bucket.getDirectory().getParentFile()
		.getAbsolutePath()
		+ "/bucket.csv";
	// TODO: Export to a special directory, which is checked for already
	// failed exports.
	executeExportTool(pathToExportTool, pathToBucket, csvOutput);
	return new File(csvOutput);
    }

    private void executeExportTool(String pathToExportTool,
	    String pathToBucket, String csvOutput) {
	try {
	    runtime.exec(
		    new String[] { pathToExportTool, pathToBucket, csvOutput,
			    "-csv" }).waitFor();
	} catch (Exception e) {
	    logExportToolCallException(pathToExportTool, pathToBucket, e);
	}
    }

    private void logExportToolCallException(String pathToExportTool,
	    String pathToBucket, Exception e) {
	logger.error(did("Called splunk's exporttool", e, "It to just work",
		"path_to_export_tool", pathToExportTool, "path_to_bucket",
		pathToBucket, "exception", e));
    }

    public static SplunkExportTool getWithLogger() {
	return new SplunkExportTool(Runtime.getRuntime());
    }

}
