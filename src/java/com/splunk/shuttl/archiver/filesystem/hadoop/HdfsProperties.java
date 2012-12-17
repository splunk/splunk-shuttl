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
package com.splunk.shuttl.archiver.filesystem.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;

/**
 * Properties for an HDFS file system.
 */
public class HdfsProperties {

	public static final String HDFS_PROPERTIES_FILENAME = "hdfs.properties";

	private final String host;
	private final String port;

	public HdfsProperties(String host, String port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public String getPort() {
		return port;
	}

	public static HdfsProperties create() {
		return create(getHdfsPropertiesFile());
	}

	public static File getHdfsPropertiesFile() {
		return BackendConfigurationFiles.create().getByName(
				HDFS_PROPERTIES_FILENAME);
	}

	public static HdfsProperties create(File hdfsProperties) {
		Properties properties = loadProperties(hdfsProperties);
		String host = properties.getProperty("hadoop.host");
		String port = properties.getProperty("hadoop.port");
		return new HdfsProperties(host, port);
	}

	private static Properties loadProperties(File hdfsProperties) {
		try {
			Properties properties = new Properties();
			properties.load(FileUtils.openInputStream(hdfsProperties));
			return properties;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
