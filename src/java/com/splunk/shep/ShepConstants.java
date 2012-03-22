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
package com.splunk.shep;

import org.apache.commons.io.FileUtils;

/**
 * @author hyan
 *
 */
public final class ShepConstants {
    public static enum SystemType {
	local, hdfs, splunk;

	public static SystemType type(String type)
		throws IllegalArgumentException {
	    if (local.toString().equals(type)) {
		return local;
	    } else if (hdfs.toString().equals(type)) {
		return hdfs;
	    } else if (splunk.toString().equals(type)) {
		return hdfs;
	    }
	    throw new IllegalArgumentException("Unsupported EventWriterType: "
		    + type);
	}
    };

    public static final String ENDPOINT_CONTEXT = "shep/rest";
    public static final String ENDPOINT_DEFAULT_HOST = "/defaulthost";
    public static final String ENDPOINT_DEFAULT_PORT = "/defaultport";
    public static final String ENDPOINT_SERVER = "/server";
    public static final String ENDPOINT_FORWARDER = "/forwarder";
    public static final String ENDPOINT_SINK_PREFIX = "/sinkprefix";
    public static final String ENDPOINT_BUCKET_ARCHIVER = "/bucket/archive";
    public static final String ENDPOINT_ARCHIVER = "/archiver";
    public static final String ENDPOINT_SHUTDOWN = "/shutdown";
    public static final String ENDPOINT_EXPORT_SRVC_STATUS = "/exportservicestatus";
    public static final String ATT_DEF_HADOOP_CLUSTER_HOST = "DefHadoopClusterHost";
    public static final String ATT_DEF_HADOOP_CLUSTER_PORT = "DefHadoopClusterPort";

    public static final String TRANSLOG_NAME = "translog";
    public static final String SPLUNK_HOME_PROPERTY = "splunk.home";
    public static final String SPLUNK_HOME = System
	    .getProperty(SPLUNK_HOME_PROPERTY);

    // check prerequisites before start the app
    static {
	if (SPLUNK_HOME == null) {
	    throw new IllegalStateException(SPLUNK_HOME_PROPERTY
		    + " is not set");
	}
    }

    public static final String SHEP_HOME = FileUtils
	    .getFile(SPLUNK_HOME, "etc", "apps", "shep").getAbsolutePath();
    public static final String TRANSLOG_DIR_PATH = FileUtils.getFile(SHEP_HOME,
	    TRANSLOG_NAME).getAbsolutePath();
    public static final String TRANSLOG_FILE_PATH = FileUtils.getFile(
	    TRANSLOG_DIR_PATH, TRANSLOG_NAME).getAbsolutePath();
    public static final String TRANSLOG_ENDTIME_KEY = "endtime";
}
