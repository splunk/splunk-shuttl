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
package com.splunk.shuttl.archiver.copy;

import java.io.File;

import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Can iterate over buckets in the cold buckets directory.
 */
public class ColdBucketInterator {

	private final Service splunkService;
	private final BucketIteratorFactory bucketIteratorFactory;

	public ColdBucketInterator(Service splunkService,
			BucketIteratorFactory bucketIteratorFactory) {
		this.splunkService = splunkService;
		this.bucketIteratorFactory = bucketIteratorFactory;
	}

	/**
	 * @return iterable over buckets in a cold buckets directory for a specified
	 *         index.
	 */
	public Iterable<LocalBucket> coldBucketsAtIndex(String indexName) {
		Index splunkIndex = getSplunkIndexByName(indexName);
		String coldPath = splunkIndex.getColdPathExpanded();
		return bucketIteratorFactory.iteratorInDirectory(new File(coldPath));
	}

	private Index getSplunkIndexByName(String indexName) {
		Index splunkIndex = splunkService.getIndexes().get(indexName);
		if (splunkIndex == null)
			throw new IllegalIndexException(indexName);
		else
			return splunkIndex;
	}

}
