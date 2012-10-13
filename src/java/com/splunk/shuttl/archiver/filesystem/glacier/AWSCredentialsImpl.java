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
package com.splunk.shuttl.archiver.filesystem.glacier;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import com.amazonaws.auth.AWSCredentials;
import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;

public class AWSCredentialsImpl implements AWSCredentials {

	public static final String AMAZON_PROPERTIES_FILENAME = "amazon.properties";

	private final String id;
	private final String secret;
	private final String endpoint;
	private final String bucket;
	private final String vault;

	public AWSCredentialsImpl(String id, String secret, String endpoint,
			String bucket, String vault) {
		this.id = id;
		this.secret = secret;
		this.endpoint = endpoint;
		this.bucket = bucket;
		this.vault = vault;
	}

	private String valueWhenNotNull(String value, String message) {
		if (value == null)
			throw new AmazonPropertyMissing(message + " Was: " + value);
		return value;
	}

	@Override
	public String getAWSAccessKeyId() {
		return valueWhenNotNull(id, "Missing AWS id property.");
	}

	@Override
	public String getAWSSecretKey() {
		return valueWhenNotNull(secret, "Missing AWS secret property.");
	}

	public String getGlacierEndpoint() {
		return valueWhenNotNull(endpoint, "Missing glacier endpoint property.");
	}

	public String getS3Bucket() {
		return valueWhenNotNull(bucket, "Missing s3 bucket property.");
	}

	public String getGlacierVault() {
		return valueWhenNotNull(vault, "Missing glacier vault property.");
	}

	public static AWSCredentialsImpl create() {
		return createWithPropertyFile(getAmazonPropertiesFile());
	}

	public static File getAmazonPropertiesFile() {
		return BackendConfigurationFiles.create().getByName(
				AWSCredentialsImpl.AMAZON_PROPERTIES_FILENAME);
	}

	/**
	 * @param properties
	 *          file containing amazon properties for all the fields.
	 */
	public static AWSCredentialsImpl createWithPropertyFile(File amazonProperties) {
		try {
			Properties properties = new Properties();
			properties.load(FileUtils.openInputStream(amazonProperties));
			String id = properties.getProperty("aws.id");
			String secret = properties.getProperty("aws.secret");
			String bucket = properties.getProperty("s3.bucket");
			String vault = properties.getProperty("glacier.vault");
			String endpoint = properties.getProperty("glacier.endpoint");

			return new AWSCredentialsImpl(id, secret, endpoint, bucket, vault);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static class AmazonPropertyMissing extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public AmazonPropertyMissing(String msg) {
			super(msg);
		}
	}
}
