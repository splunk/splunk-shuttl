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
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;

/**
 * Properties for an HDFS file system.
 */
public class HdfsProperties {


	private static final Logger logger = Logger.getLogger(HdfsProperties.class);

	public static final String FS_DEFAULT_NAME = "fs.default.name";
	public static final String HDFS_PROPERTIES_FILENAME = "hdfs.properties";

	private final Configuration conf;

	public HdfsProperties(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
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
		Configuration conf = getConf(properties);
		setUriOldStyle(conf, host, port);
		return new HdfsProperties(conf);
	}

	private static void setUriOldStyle(Configuration conf, String host,
			String port) {
		if (host == null) {
			return;
		} else {
			String uri = "hdfs://" + host;
			if (port != null)
				uri += ":" + port;
			logger.warn("Setting hdfs URI=" + uri
					+ ", with the deprecated hadoop.host and hadoop.port style. "
					+ "Since hdfs configuration supports arbitrary key-values, consider "
					+ "using fs.default.name instead.");
			conf.set(FS_DEFAULT_NAME, uri);
		}
	}

	private static Configuration getConf(Properties properties) {
		Configuration conf = new Configuration();
		for (Entry<Object, Object> e : properties.entrySet()) {
			conf.set(e.getKey().toString(), e.getValue().toString());
		}
		return conf;
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
