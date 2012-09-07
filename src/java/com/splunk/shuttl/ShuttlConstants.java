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
package com.splunk.shuttl;

import org.apache.commons.io.FileUtils;

/**
 * @author hyan
 * 
 */
public final class ShuttlConstants {
	public static enum SystemType {
		local, hdfs, splunk;
	};

	public static enum OutputMode {
		xml, json, csv, raw;
	}

	// REST endpoints
	public static final String ENDPOINT_CONTEXT = "shuttl/rest";
	public static final String ENDPOINT_SHUTTL_HOST = "/defaulthost";
	public static final String ENDPOINT_SHUTTL_PORT = "/defaultport";
	public static final String ENDPOINT_SERVER = "/server";
	public static final String ENDPOINT_FORWARDER = "/forwarder";
	public static final String ENDPOINT_SINK_PREFIX = "/sinkprefix";
	public static final String ENDPOINT_BUCKET_ARCHIVER = "/bucket/archive";
	public static final String ENDPOINT_BUCKET_THAW = "/bucket/thaw";
	public static final String ENDPOINT_BUCKET_FLUSH = "/bucket/flush";
	public static final String ENDPOINT_THAW_LIST = "/thaw/list";
	public static final String ENDPOINT_LIST_BUCKETS = "/bucket/list";
	public static final String ENDPOINT_LIST_INDEXES = "/index/list";
	public static final String ENDPOINT_ARCHIVER = "/archiver";
	public static final String ENDPOINT_SHUTDOWN = "/shutdown";
	public static final String ENDPOINT_EXPORT_SRVC_STATUS = "/exportservicestatus";
	public static final String ATT_DEF_HADOOP_CLUSTER_HOST = "DefHadoopClusterHost";
	public static final String ATT_DEF_HADOOP_CLUSTER_PORT = "DefHadoopClusterPort";

	// Splunk field names
	public static final String SPLUNK_EVENT_FIELD_RAW = "_raw";
	public static final String SPLUNK_EVENT_FIELD_SOURCE = "source";
	public static final String SPLUNK_EVENT_FIELD_SOURCETYPE = "_sourcetype";
	public static final String SPLUNK_EVENT_FIELD_TIME = "_time";

	// properties
	public static final String SPLUNK_HOME_PROPERTY = "splunk.home";
	public static final String SPLUNK_USER_NAME = "splunk.username";
	public static final String SPLUNK_PASSWORD = "splunk.password";
	public static final String SPLUNK_HOST = "splunk.host";
	public static final String SPLUNK_MGMT_PORT = "splunk.mgmtport";

	// file names
	public static final String SHUTTL_DEFAULT_PROPERTIES_FILE_NAME = "default.properties";
	public static final String TRANSLOG_NAME = "translog";

	public static final String SPLUNK_HOME = System
			.getProperty(SPLUNK_HOME_PROPERTY);

	// check prerequisites before start the app
	static {
		if (SPLUNK_HOME == null)
			throw new IllegalStateException(SPLUNK_HOME_PROPERTY + " is not set");
	}

	public static final String SHUTTL_HOME = FileUtils.getFile(SPLUNK_HOME,
			"etc", "apps", "shuttl").getAbsolutePath();
	public static final String TRANSLOG_DIR_PATH = FileUtils.getFile(SHUTTL_HOME,
			TRANSLOG_NAME).getAbsolutePath();
	public static final String TRANSLOG_FILE_PATH = FileUtils.getFile(
			TRANSLOG_DIR_PATH, TRANSLOG_NAME).getAbsolutePath();
	public static final String TRANSLOG_ENDTIME_KEY = "endtime";

}
