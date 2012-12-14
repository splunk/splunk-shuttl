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

import com.splunk.shuttl.archiver.model.LocalBucket;

public class BucketIteratorFactory {

	/**
	 * @return iterator over buckets in a directory.
	 */
	public Iterable<LocalBucket> iteratorInDirectory(File eq) {
		throw new UnsupportedOperationException();
	}

}
