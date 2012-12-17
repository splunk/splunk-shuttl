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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.metastore.MetadataStore;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class GlacierArchiveIdStoreTest {

	private GlacierArchiveIdStore glacierArchiveIdStore;
	private MetadataStore metadataStore;
	private String archiveId;

	@BeforeMethod
	public void setUp() {
		metadataStore = mock(MetadataStore.class);
		glacierArchiveIdStore = new GlacierArchiveIdStore(metadataStore);

		archiveId = "archiveId";
	}

	public void getFileName_givenName_shouldNotChangeToKeepCompatability() {
		String name = glacierArchiveIdStore.getFileName();
		assertEquals(name, "glacier.archiveid.meta");
	}

	public void putArchiveId_bucketAndId_putsArchiveIdWithMetadataStore() {
		Bucket bucket = TUtilsBucket.createBucket();
		glacierArchiveIdStore.putArchiveId(bucket, archiveId);
		verify(metadataStore).put(bucket, glacierArchiveIdStore.getFileName(),
				archiveId);
	}

	public void getArchiveId_bucket_getsArchiveId() {
		Bucket bucket = mock(Bucket.class);
		when(metadataStore.read(bucket, glacierArchiveIdStore.getFileName()))
				.thenReturn(archiveId);
		assertEquals(archiveId, glacierArchiveIdStore.getArchiveId(bucket));
	}
}
