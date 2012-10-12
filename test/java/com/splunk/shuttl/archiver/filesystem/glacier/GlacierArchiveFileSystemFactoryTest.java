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

import static org.testng.Assert.*;

import java.net.URI;

import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class GlacierArchiveFileSystemFactoryTest {

	private static final String ID = "id";
	private static final String SECRET = "secret";
	private static final String ENDPOINT = "aws.endpoint.com";
	private static final String BUCKET = "bucket";
	private static final String VAULT = "vault_name";

	@Test(groups = { "fast-unit" }, enabled = false)
	public void getCredentials_givenValidUri_createsFileSystem() {
		AWSCredentialsImpl credentials = GlacierArchiveFileSystemFactory
				.getCredentials(null);
		assertEquals(ID, credentials.getAWSAccessKeyId());
		assertEquals(SECRET, credentials.getAWSSecretKey());
		assertEquals(ENDPOINT, credentials.getEndpoint());
		assertEquals(BUCKET, credentials.getBucket());
		assertEquals(VAULT, credentials.getVault());
	}

	@Test(expectedExceptions = { InvalidGlacierUriException.class })
	public void getCredentials_givenNoId_throws() {
		testMissingUriPart(ID);
	}

	private void testMissingUriPart(String uriPart) {
		String noId = getBackendName().toString().replaceAll(uriPart, "");
		assertFalse(noId.contains(uriPart));
		GlacierArchiveFileSystemFactory.getCredentials(URI.create(noId));
	}

	@Test(expectedExceptions = { InvalidGlacierUriException.class })
	public void getCredentials_givenNoSecret_throws() {
		testMissingUriPart(SECRET);
	}

	@Test(expectedExceptions = { InvalidGlacierUriException.class })
	public void getCredentials_givenNoEndpoint_throws() {
		testMissingUriPart(ENDPOINT);
	}

	@Test(expectedExceptions = { InvalidGlacierUriException.class })
	public void getCredentials_givenNoBucket_throws() {
		testMissingUriPart(BUCKET);
	}

	@Test(expectedExceptions = { InvalidGlacierUriException.class })
	public void getCredentials_givenNoVault_throws() {
		testMissingUriPart(VAULT);
	}

	/**
	 * @return valid uri for creating a {@link GlacierArchiveFileSystem}
	 */
	public static String getBackendName() {
		return "glacier";
	}

}
