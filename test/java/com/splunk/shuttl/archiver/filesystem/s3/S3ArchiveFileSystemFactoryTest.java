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
package com.splunk.shuttl.archiver.filesystem.s3;

import static org.testng.Assert.*;

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.glacier.AWSCredentialsImpl;

@Test(groups = { "fast-unit" })
public class S3ArchiveFileSystemFactoryTest {

	@BeforeMethod
	public void setUp() {

	}

	public void s3UriForHadoop__putsDataInTheRightOrder() {
		URI uri = S3ArchiveFileSystemFactory.createS3UriForHadoopFileSystem(
				"scheme", new AWSCredentialsImpl("id", "secret", null, "bucket", null));
		assertEquals(uri.getScheme(), "scheme");
		assertEquals(uri.getUserInfo(), "id:secret");
		assertEquals(uri.getHost(), "bucket");
	}

	public void s3UriForHadoop_secretWithSpecialSymbols_validUri() {
		URI uri = S3ArchiveFileSystemFactory.createS3UriForHadoopFileSystem(
				"scheme",
				new AWSCredentialsImpl("id", "se+cr/et", null, "bucket", null));
		assertEquals(uri.getScheme(), "scheme");
		assertEquals(uri.getUserInfo(), "id:se+cr/et");
		assertEquals(uri.getHost(), "bucket");
	}
}
