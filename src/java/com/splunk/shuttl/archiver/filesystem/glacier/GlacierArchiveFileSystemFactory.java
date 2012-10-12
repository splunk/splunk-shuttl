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
import java.net.URI;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;
import com.splunk.shuttl.archiver.filesystem.s3.S3ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.importexport.tgz.CreatesBucketTgz;
import com.splunk.shuttl.archiver.importexport.tgz.TgzFormatExporter;
import com.splunk.shuttl.archiver.util.GroupRegex;
import com.splunk.shuttl.archiver.util.IllegalRegexGroupException;

/**
 * Construction logic for creating {@link GlacierArchiveFileSystem}
 */
public class GlacierArchiveFileSystemFactory {

	private static final String GLACIER_PROPERTIES_FILENAME = "amazon.properties";

	/**
	 * @throws UnsupportedGlacierUriException
	 *           - if the format is not valid.
	 */
	public static GlacierArchiveFileSystem create(
			LocalFileSystemPaths localFileSystemPaths) {
		return create(localFileSystemPaths, BackendConfigurationFiles.create()
				.getByName(GLACIER_PROPERTIES_FILENAME));
	}

	public static GlacierArchiveFileSystem create(
			LocalFileSystemPaths localFileSystemPaths, File amazonProperties) {
		AWSCredentialsImpl credentials = getCredentials(amazonProperties);
		GlacierClient glacierClient = GlacierClient.create(credentials);

		ArchiveFileSystem s3 = S3ArchiveFileSystemFactory.createS3n();
		TgzFormatExporter tgzFormatExporter = TgzFormatExporter
				.create(CreatesBucketTgz.create(localFileSystemPaths.getTgzDirectory()));
		Logger logger = Logger.getLogger(GlacierArchiveFileSystem.class);
		BucketDeleter bucketDeleter = BucketDeleter.create();

		return new GlacierArchiveFileSystem(s3, glacierClient, tgzFormatExporter,
				logger, bucketDeleter);
	}

	/**
	 * @return AWSCredentials taken from the amazonProperties file.
	 */
	public static AWSCredentialsImpl getCredentials(File amazonProperties) {
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

	@SuppressWarnings("unused")
	private static class CredentialsParser {

		public static final String LEGAL_URI_REGEX = "glacier://(.+?):(.+?)@(.+?):(.+?)/(.+)";

		private final URI uri;
		public String id = "";
		public String secret = "";
		public String endpoint = "";
		public String bucket = "";
		public String vault = "";

		public CredentialsParser(URI uri) {
			this.uri = uri;
		}

		public CredentialsParser parse() {
			try {
				return doParse();
			} catch (IllegalRegexGroupException e) {
				throw new InvalidGlacierConfigurationException(uri);
			}
		}

		private CredentialsParser doParse() {
			GroupRegex regex = new GroupRegex(LEGAL_URI_REGEX, uri.toString());
			id = regex.getValue(1);
			secret = regex.getValue(2);
			endpoint = regex.getValue(3);
			bucket = regex.getValue(4);
			vault = regex.getValue(5);
			return this;
		}

	}

}
