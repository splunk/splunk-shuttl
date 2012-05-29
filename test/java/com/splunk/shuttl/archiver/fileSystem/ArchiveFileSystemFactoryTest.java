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
package com.splunk.shuttl.archiver.fileSystem;

import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class ArchiveFileSystemFactoryTest {

	@Test(groups = { "fast-unit" })
	public void isSupportedUri_givenFileUri_true() {
		assertTrue(ArchiveFileSystemFactory.isSupportedUri(URI.create("file:/")));
	}

	@Test(groups = { "fast-unit" })
	public void isSupportedUri_givenUnSupportedUri_false() {
		assertFalse(ArchiveFileSystemFactory.isSupportedUri(URI
				.create("unsupported:/uri")));
	}

	@Test(groups = { "slow-unit" })
	public void getForUriToTmpDir_givenLocalFileURI_nonNullFileSystem() {
		URI localUri = URI.create("file:/tmp");
		ArchiveFileSystem fileSystem = ArchiveFileSystemFactory
				.getForUriToTmpDir(localUri);
		assertNotNull(fileSystem);
	}

	@Test(groups = { "end-to-end" })
	@Parameters({ "hadoop.host", "hadoop.port" })
	public void getForUriToTmpDir_givenHdfsUri_nonNullFileSystem(String host,
			String port) {
		URI localUri = URI.create("hdfs://" + host + ":" + port + "/tmp");
		ArchiveFileSystem fileSystem = ArchiveFileSystemFactory
				.getForUriToTmpDir(localUri);
		assertNotNull(fileSystem);
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { UnsupportedUriException.class })
	public void getForUriToTmpDir_givenUnsupportedUri_throwUnsupportedUriException() {
		ArchiveFileSystemFactory.getForUriToTmpDir(URI.create("unsupported:/uri"));
	}
}
