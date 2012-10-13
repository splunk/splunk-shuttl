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
package com.splunk.shuttl.server.mbeans;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.filesystem.glacier.AWSCredentialsImpl;
import com.splunk.shuttl.archiver.filesystem.hadoop.HdfsProperties;
import com.splunk.shuttl.server.model.ArchiverConf;

/**
 * Overrides configuration with the old version using ArchiveRootURI.
 */
public class OverrideWithOldArchiverRootURIConfiguration {

	private static final Logger logger = Logger
			.getLogger(OverrideWithOldArchiverRootURIConfiguration.class);
	private static final String overrideReason = "Overriding because of use of"
			+ " old configuration with ArchiveRootURI.";

	private final ArchiverConf conf;
	private final URI archiverRootURI;

	public OverrideWithOldArchiverRootURIConfiguration(ArchiverConf conf) {
		this.conf = conf;
		this.archiverRootURI = getArchiverRootURI();
	}

	private URI getArchiverRootURI() {
		try {
			return new URI(conf.getArchiverRootURI());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Overriding configuration by setting values and writing the conf files
	 * necessary.
	 */
	public void override() {
		logger.warn("Using old version of configration with " + "ArchiveRootURI: "
				+ archiverRootURI + ".\nOverride any other configuration done.");
		setConfigurationValues();
		overridePropertyFiles();
	}

	private void setConfigurationValues() {
		if (conf.getArchivePath() == null)
			conf.setArchivePath(archiverRootURI.getPath());
		if (conf.getBackendName() == null)
			conf.setBackendName(archiverRootURI.getScheme());
	}

	private void overridePropertyFiles() {
		String scheme = archiverRootURI.getScheme();
		if (scheme.equals("hdfs"))
			overrideHdfs();
		else if (scheme.equals("s3") || scheme.equals("s3n"))
			overrideS3orS3n();
		else
			throw new ShuttlMBeanException(
					"Does not now how to configure with ArchiverRootURI: "
							+ archiverRootURI);
	}

	private void overrideHdfs() {
		overrideHdfsProperties(archiverRootURI.getHost(),
				archiverRootURI.getPort(), overrideReason);
	}

	private void overrideHdfsProperties(String host, int port, String reason) {
		try {
			Properties props = new Properties();
			props.setProperty("hadoop.host", host);
			props.setProperty("hadoop.port", "" + port);
			File propertiesFile = HdfsProperties.getHdfsPropertiesFile();
			writeToPropertiesFile(props, propertiesFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void overrideS3orS3n() {
		String[] split = archiverRootURI.getUserInfo().split(":");
		String id = split[0];
		String secret = split[1];
		String bucket = archiverRootURI.getHost();
		overrideS3Properties(id, secret, bucket, overrideReason);
	}

	private void overrideS3Properties(String id, String secret, String bucket,
			String reason) {
		try {
			Properties props = new Properties();
			props.setProperty("aws.id", id);
			props.setProperty("aws.secret", secret);
			props.setProperty("s3.bucket", bucket);
			File propertiesFile = AWSCredentialsImpl.getAmazonPropertiesFile();
			writeToPropertiesFile(props, propertiesFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void writeToPropertiesFile(Properties props, File file)
			throws IOException {
		logger.warn("Writing properties: " + props + ", to properties file: "
				+ file + ", overriding any previous configuration.");
		props.store(FileUtils.openOutputStream(file), overrideReason);
	}
}
