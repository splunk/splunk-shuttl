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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;
import com.splunk.shuttl.archiver.filesystem.glacier.AWSCredentialsImpl.AmazonPropertyMissing;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsFile;

public class AWSCredentialsImplTest {

	String id = "id";
	String secret = "secret";
	String endpoint = "endpoint";
	String bucket = "bucket";
	String vault = "vault";
	private String[] amazonProps = new String[] { "aws.id=" + id,
			"aws.secret=" + secret, "glacier.endpoint=" + endpoint,
			"glacier.vault=" + vault, "s3.bucket=" + bucket };

	@Test(groups = { "fast-unit" })
	public void _givenIdAndSecret_getsCredentials() {
		AWSCredentialsImpl credentials = new AWSCredentialsImpl(id, secret,
				endpoint, bucket, vault);
		assertValidCredentials(credentials);
	}

	private void assertValidCredentials(AWSCredentialsImpl credentials) {
		assertEquals(id, credentials.getAWSAccessKeyId());
		assertEquals(secret, credentials.getAWSSecretKey());
		assertEquals(endpoint, credentials.getGlacierEndpoint());
		assertEquals(bucket, credentials.getS3Bucket());
		assertEquals(vault, credentials.getGlacierVault());
	}

	@Test(groups = { "fast-unit" })
	public void _givenPropertyFile_createsCredentials() {
		File properties = getPropertiesFile(amazonProps);

		AWSCredentialsImpl creds = AWSCredentialsImpl
				.createWithPropertyFile(properties);
		assertValidCredentials(creds);
	}

	private File getPropertiesFile(String... kvs) {
		File properties = createFile();
		TUtilsFile.writeKeyValueProperties(properties, kvs);
		return properties;
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { AmazonPropertyMissing.class })
	public void _missingId_throwsWhenGettingValue() {
		String[] missingId = getKvsWithoutProperty("aws.id");
		AWSCredentialsImpl creds = AWSCredentialsImpl
				.createWithPropertyFile(getPropertiesFile(missingId));
		creds.getAWSAccessKeyId();
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { AmazonPropertyMissing.class })
	public void _missingSecret_throwsWhenGettingValue() {
		AWSCredentialsImpl creds = AWSCredentialsImpl
				.createWithPropertyFile(getPropertiesFile(getKvsWithoutProperty("aws.secret")));
		creds.getAWSSecretKey();
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { AmazonPropertyMissing.class })
	public void _missingBucket_throwsWhenGettingValue() {
		AWSCredentialsImpl creds = AWSCredentialsImpl
				.createWithPropertyFile(getPropertiesFile(getKvsWithoutProperty("s3.bucket")));
		creds.getS3Bucket();
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { AmazonPropertyMissing.class })
	public void _missingEndpoint_throwsWhenGettingValue() {
		AWSCredentialsImpl creds = AWSCredentialsImpl
				.createWithPropertyFile(getPropertiesFile(getKvsWithoutProperty("glacier.endpoint")));
		creds.getGlacierEndpoint();
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { AmazonPropertyMissing.class })
	public void _missingVault_throwsWhenGettingValue() {
		AWSCredentialsImpl creds = AWSCredentialsImpl
				.createWithPropertyFile(getPropertiesFile(getKvsWithoutProperty("glacier.vault")));
		creds.getGlacierVault();
	}

	private String[] getKvsWithoutProperty(String propertyToNotHave) {
		String[] missingId = new String[amazonProps.length - 1];
		for (int i = 0, j = 0; i < amazonProps.length; i++)
			if (!amazonProps[i].contains(propertyToNotHave))
				missingId[j++] = amazonProps[i];
		return missingId;
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void _givenRealAmazonPropertyFile_gettersAllHaveNonNullValues(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				File amazonProperties = BackendConfigurationFiles.create().getByName(
						AWSCredentialsImpl.AMAZON_PROPERTIES_FILENAME);
				AWSCredentialsImpl creds = AWSCredentialsImpl
						.createWithPropertyFile(amazonProperties);
				assertGettersAreNonNullValues(creds);
			}
		});
	}

	private void assertGettersAreNonNullValues(AWSCredentialsImpl creds) {
		assertNotNull(creds.getAWSAccessKeyId());
		assertNotNull(creds.getAWSSecretKey());
		assertNotNull(creds.getS3Bucket());
		assertNotNull(creds.getGlacierEndpoint());
		assertNotNull(creds.getGlacierVault());
	}
}
