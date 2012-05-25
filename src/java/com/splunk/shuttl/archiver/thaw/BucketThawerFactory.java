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

import com.splunk.Service;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;

/**
 * Factory for getting {@link BucketThawer}
 */
public class BucketThawerFactory {

    public static BucketThawer createDefaultThawer() {
	Service splunkService = getLoggedInSplunkService();
	SplunkSettings splunkSettings = getSplunkSettings(splunkService);
	ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
	return createWithSplunkSettingsAndConfig(splunkSettings, config);
    }

    public static BucketThawer createWithSplunkSettingsAndConfig(
	    SplunkSettings splunkSettings, ArchiveConfiguration configuration) {
	ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
		.getWithConfiguration(configuration);
	PathResolver pathResolver = new PathResolver(configuration);

	ArchiveBucketsLister bucketsLister = bucketLister(archiveFileSystem,
		pathResolver);
	BucketFilter bucketFilter = new BucketFilter();
	BucketFormatResolver bucketFormatResolver = getBucketFormatResolver(
		archiveFileSystem, configuration, pathResolver);
	ThawBucketTransferer thawBucketTransferer = getThawBucketTransferer(
		archiveFileSystem, splunkSettings);
	return new BucketThawer(bucketsLister, bucketFilter,
		bucketFormatResolver, thawBucketTransferer,
		BucketRestorer.create());
    }

    private static ArchiveBucketsLister bucketLister(
	    ArchiveFileSystem archiveFileSystem, PathResolver pathResolver) {
	ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
		pathResolver, archiveFileSystem);
	ArchiveBucketsLister bucketsLister = new ArchiveBucketsLister(
		archiveFileSystem, indexesLister, pathResolver);
	return bucketsLister;
    }

    private static BucketFormatResolver getBucketFormatResolver(
	    ArchiveFileSystem archiveFileSystem,
	    ArchiveConfiguration archiveConfiguration, PathResolver pathResolver) {
	BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(
		archiveConfiguration);
	BucketFormatResolver bucketFormatResolver = new BucketFormatResolver(
		pathResolver, archiveFileSystem, bucketFormatChooser);
	return bucketFormatResolver;
    }

    private static ThawBucketTransferer getThawBucketTransferer(
	    ArchiveFileSystem archiveFileSystem, SplunkSettings splunkSettings) {
	ThawLocationProvider thawLocationProvider = new ThawLocationProvider(
		splunkSettings);
	ThawBucketTransferer thawBucketTransferer = new ThawBucketTransferer(
		thawLocationProvider, archiveFileSystem);
	return thawBucketTransferer;
    }

    // TODO: Communicating with splunk through splunk home is not nice.
    // CONFIG
    private static Service getLoggedInSplunkService() {
	Service splunkService = new Service("localhost", 8089);
	splunkService.login("admin", "changeme");
	return splunkService;
    }

    /**
     * @return
     */
    public static SplunkSettings getSplunkSettings(Service splunkService) {
	return new SplunkSettings(splunkService);
    }
}
