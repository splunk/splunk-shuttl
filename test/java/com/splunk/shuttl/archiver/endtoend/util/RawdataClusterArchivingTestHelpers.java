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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

import com.splunk.shuttl.archiver.endtoend.ClusterReplicatedBucketArchivingTest.ArchivePathAsserter;
import com.splunk.shuttl.archiver.endtoend.ClusterReplicatedBucketArchivingTest.ReplicatedBucketProvider;
import com.splunk.shuttl.archiver.endtoend.util.ReplicatedClusterArchivingTestHelpers.FullReplicatedBucketProvider;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.LocalBucket;

public class RawdataClusterArchivingTestHelpers {

	public static class RawdataOnlyReplicatedBucketProvider implements
			ReplicatedBucketProvider {
		private final FullReplicatedBucketProvider rbProvider;

		public RawdataOnlyReplicatedBucketProvider(
				FullReplicatedBucketProvider rbProvider) {
			this.rbProvider = rbProvider;
		}

		@Override
		public LocalBucket create(String coldPathExpanded, String slave1Guid) {
			LocalBucket fullReplicatedBucket = rbProvider.create(coldPathExpanded,
					slave1Guid);
			deleteAllFilesExceptRawdataAndDotFiles(fullReplicatedBucket);
			return fullReplicatedBucket;
		}

		private void deleteAllFilesExceptRawdataAndDotFiles(
				LocalBucket fullReplicatedBucket) {
			File[] filesExceptRawdataAndDotFiles = getFilesExceptRawdataAndDotFiles(fullReplicatedBucket);
			assertFalse(filesExceptRawdataAndDotFiles.length == 0);
			System.out.println(Arrays.toString(filesExceptRawdataAndDotFiles));
			for (File f : filesExceptRawdataAndDotFiles)
				assertTrue(f.delete(), "Could not delete: " + f.getAbsolutePath());
			System.out.println(Arrays.toString(fullReplicatedBucket.getDirectory()
					.listFiles()));
		}

		private File[] getFilesExceptRawdataAndDotFiles(
				LocalBucket fullReplicatedBucket) {
			File[] filesInBucketExceptRawdataAndDotFiles = fullReplicatedBucket
					.getDirectory().listFiles(new FileFilter() {

						@Override
						public boolean accept(File f) {
							if (isRawdataOrDotFile(f.getName()))
								return false;
							else
								return true;
						}

						private boolean isRawdataOrDotFile(String fileName) {
							return fileName.equals("rawdata") || fileName.startsWith(".");
						}
					});
			return filesInBucketExceptRawdataAndDotFiles;
		}
	}

	public static class AssertsArchivePathDoesNotExist implements
			ArchivePathAsserter {

		@Override
		public void assertStateOfArchivePathOnFileSystem(String archivePath,
				ArchiveFileSystem archiveFileSystem) throws IOException {
			assertFalse(archiveFileSystem.exists(archivePath),
					"archiveFileSystem.exists(archivePath) was true, with archivePath: "
							+ archivePath);
		}
	}
}
