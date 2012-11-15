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
package com.splunk.shuttl.archiver.listers;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;

/**
 * Factory for creating {@link ArchiveBucketsLister}
 */
public class ArchiveBucketsListerFactory {

	/**
	 * @return instance configured with the configuration.
	 */
	public static ArchiveBucketsLister create(ArchiveConfiguration config) {
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		PathResolver pathResolver = new PathResolver(config);

		ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
				pathResolver, archiveFileSystem);
		return new ArchiveBucketsLister(archiveFileSystem, indexesLister,
				pathResolver);
	}

}
