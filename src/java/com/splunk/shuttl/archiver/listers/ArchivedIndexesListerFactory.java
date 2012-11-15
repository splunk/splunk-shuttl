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
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;

public class ArchivedIndexesListerFactory {

	/**
	 * @return configured {@link ArchivedIndexesLister} instance.
	 */
	public static ArchivedIndexesLister create() {
		return create(ArchiveConfiguration.getSharedInstance());
	}

	/**
	 * @param config
	 * @return
	 */
	public static ArchivedIndexesLister create(ArchiveConfiguration config) {
		return new ArchivedIndexesLister(new PathResolver(config),
				ArchiveFileSystemFactory.getWithConfiguration(config));
	}

}
