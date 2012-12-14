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
package com.splunk.shuttl.archiver.thaw;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.importexport.BucketImportController;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.listers.ListsBucketsFilteredFactory;
import com.splunk.shuttl.archiver.metastore.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.BucketFactory;

/**
 * Factory for getting {@link BucketThawer}
 */
public class BucketThawerFactory {

	/**
	 * Default {@link BucketThawer} as configured with .conf files.
	 */
	public static BucketThawer createDefaultThawer() {
		SplunkSettings splunkSettings = SplunkSettingsFactory.create();
		ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
		return createWithConfigAndSplunkSettingsAndLocalFileSystemPaths(config,
				splunkSettings, LocalFileSystemPaths.create());
	}

	/**
	 * Factory method for testability.
	 */
	public static BucketThawer createWithConfigAndSplunkSettingsAndLocalFileSystemPaths(
			ArchiveConfiguration configuration, SplunkSettings splunkSettings,
			LocalFileSystemPaths localFileSystemPaths) {
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(configuration);
		return create(configuration, splunkSettings, localFileSystemPaths,
				archiveFileSystem);
	}

	public static BucketThawer create(ArchiveConfiguration configuration,
			SplunkSettings splunkSettings, LocalFileSystemPaths localFileSystemPaths,
			ArchiveFileSystem archiveFileSystem) {
		ThawLocationProvider thawLocationProvider = new ThawLocationProvider(
				splunkSettings, localFileSystemPaths);

		ThawBucketTransferer thawBucketTransferer = getThawBucketTransferer(
				archiveFileSystem, thawLocationProvider);
		ListsBucketsFiltered listsBucketsFiltered = ListsBucketsFilteredFactory
				.create(configuration);
		PathResolver pathResolver = new PathResolver(configuration);
		BucketSizeResolver bucketSizeResolver = new BucketSizeResolver(
				ArchiveBucketSize.create(pathResolver, archiveFileSystem,
						localFileSystemPaths));
		GetsBucketsFromArchive getsBucketsFromArchive = new GetsBucketsFromArchive(
				thawBucketTransferer, BucketImportController.create(),
				bucketSizeResolver);
		return new BucketThawer(listsBucketsFiltered, getsBucketsFromArchive,
				new LocalBucketStorage(thawLocationProvider), new ThawBucketLocker(
						localFileSystemPaths));
	}

	private static ThawBucketTransferer getThawBucketTransferer(
			ArchiveFileSystem archiveFileSystem,
			ThawLocationProvider thawLocationProvider) {
		ThawBucketTransferer thawBucketTransferer = new ThawBucketTransferer(
				thawLocationProvider, archiveFileSystem, new BucketFactory(),
				new TransactionExecuter());
		return thawBucketTransferer;
	}
}
