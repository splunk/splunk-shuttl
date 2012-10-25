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
package com.splunk.shuttl.archiver.metastore;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.metastore.MetadataStore.CouldNotReadMetadataException;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ArchiveBucketSizeTest {

	private ArchiveBucketSize archiveBucketSize;
	private MetadataStore metadataStore;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		metadataStore = mock(MetadataStore.class);
		archiveBucketSize = new ArchiveBucketSize(metadataStore);
		bucket = TUtilsBucket.createBucket();
	}

	public void readBucketSizeMetadataFileName__fileNameIsPathResolversBucketSizeFileNameForOlderShuttlCompatibillity() {
		assertEquals(archiveBucketSize.getSizeMetadataFileName(),
				PathResolver.BUCKET_SIZE_FILE_NAME);
	}

	public void persistBucketSize_givenBucket_putsBucketWithMetadataStore() {
		archiveBucketSize.persistBucketSize(bucket);
		verify(metadataStore).put(bucket,
				archiveBucketSize.getSizeMetadataFileName(), "" + bucket.getSize());
	}

	public void readBucketSize_givenBucket_sizeFromMetadataStore() {
		String data = "123";
		Long longData = 123L;
		when(
				metadataStore.read(bucket, archiveBucketSize.getSizeMetadataFileName()))
				.thenReturn(data);
		assertEquals(longData, archiveBucketSize.readBucketSize(bucket));
	}

	public void readBucketSize_metadataStoreException_null() {
		when(metadataStore.read(any(Bucket.class), anyString())).thenThrow(
				new CouldNotReadMetadataException());
		assertEquals(null, archiveBucketSize.readBucketSize(bucket));
	}

}
