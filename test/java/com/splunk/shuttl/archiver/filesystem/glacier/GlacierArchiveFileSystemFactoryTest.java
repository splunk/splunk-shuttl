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

@Test(groups = { "fast-unit" }, enabled = false)
public class GlacierArchiveFileSystemFactoryTest {

	private static final String ID = "id";
	private static final String SECRET = "secret";
	private static final String ENDPOINT = "aws.endpoint.com";
	private static final String VAULT = "vault_name";

	public void _givenValidUri_createsFileSystem() {
		GlacierArchiveFileSystem glacier = GlacierArchiveFileSystemFactory
				.create(getValidUri());
		assertEquals(ID, glacier.getId());
		assertEquals(SECRET, glacier.getSecret());
		assertEquals(ENDPOINT, glacier.getEndpoint());
		assertEquals(VAULT, glacier.getVault());
	}

	/**
	 * @return valid uri for creating a {@link GlacierArchiveFileSystem}
	 */
	public static URI getValidUri() {
		return URI.create("glacier://" + ID + ":" + SECRET + "@" + ENDPOINT + "/"
				+ VAULT);
	}

}
