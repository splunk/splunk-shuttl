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
package com.splunk.shuttl.archiver.endtoend.util;

import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import com.splunk.shuttl.archiver.endtoend.ClusterReplicatedBucketArchivingTest;
import com.splunk.shuttl.archiver.endtoend.ClusterReplicatedBucketArchivingTest.ArchivePathAsserter;
import com.splunk.shuttl.archiver.endtoend.ClusterReplicatedBucketArchivingTest.ReplicatedBucketProvider;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

/**
 * Used with {@link ClusterReplicatedBucketArchivingTest}
 */
public class ReplicatedClusterArchivingTestHelpers {

	public static class FullReplicatedBucketProvider implements
			ReplicatedBucketProvider {

		private final String index;

		public FullReplicatedBucketProvider(String index) {
			this.index = index;
		}

		@Override
		public LocalBucket create(String coldPathExpanded, String slave1Guid) {
			return TUtilsBucket.createRealReplicatedBucket(index, new File(
					coldPathExpanded), slave1Guid);
		}
	}

	public static class AssertsArchivePathExists implements ArchivePathAsserter {

		@Override
		public void assertStateOfArchivePathOnFileSystem(String archivePath,
				ArchiveFileSystem archiveFileSystem) throws IOException {
			assertTrue(archiveFileSystem.exists(archivePath),
					"archiveFileSystem.exists(archivePath) was false, with archivePath: "
							+ archivePath);
		}
	}
}
