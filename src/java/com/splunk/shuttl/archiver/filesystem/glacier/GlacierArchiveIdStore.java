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

import com.splunk.shuttl.archiver.metastore.MetadataStore;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Persists and gets the glacier archive id for a bucket. @see
 * {@link MetadataStore} for how it's persisted and fetched.
 */
public class GlacierArchiveIdStore {

	private final MetadataStore metadataStore;

	public GlacierArchiveIdStore(MetadataStore metadataStore) {
		this.metadataStore = metadataStore;
	}

	/**
	 * Associates the archive id to the bucket by persisting the data using
	 * {@link MetadataStore}.
	 */
	public void putArchiveId(Bucket bucket, String archiveId) {
		metadataStore.put(bucket, getFileName(), archiveId);
	}

	/**
	 * @return ArchiveId persisted to a bucket, using {@link MetadataStore}.
	 */
	public String getArchiveId(Bucket bucket) {
		return metadataStore.read(bucket, getFileName());
	}

	/**
	 * @return file name for the archive id metadata file.
	 */
	public String getFileName() {
		return "glacier.archiveid.meta";
	}
}
