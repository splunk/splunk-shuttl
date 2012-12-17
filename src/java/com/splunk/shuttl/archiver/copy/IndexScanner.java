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
import java.util.Map;

import com.splunk.Service;
import com.splunk.shuttl.archiver.thaw.SplunkIndexesLayer;

public class IndexScanner {

	private IndexStoragePaths indexPaths;

	public IndexScanner(IndexStoragePaths indexPaths) {
		this.indexPaths = indexPaths;
	}

	/**
	 * @return index of the path to a bucket.
	 * @throws {@link UnknownIndexPathException} if an index could not be found.
	 */
	public String getIndex(File bucketPath) {
		Map<String, String> paths = indexPaths.getIndexPaths();
		String bucketsParent = bucketPath.getParent();
		if (paths.containsKey(bucketsParent))
			return paths.get(bucketsParent);
		else
			throw new UnknownIndexPathException(bucketPath);
	}

	public static String getIndexNameByBucketPath(File bucketDir,
			Service splunkService) {
		return new IndexScanner(new IndexStoragePaths(new SplunkIndexesLayer(
				splunkService))).getIndex(bucketDir);
	}

	public static class UnknownIndexPathException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public UnknownIndexPathException(File bucketPath) {
			super("Unknown index with bucket path: " + bucketPath.getAbsolutePath());
		}
	}
}
