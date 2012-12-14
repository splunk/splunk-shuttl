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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;

/**
 * Paths where indexes store buckets. Cold path and home path (hot, warm, cold
 * buckets).
 */
public class IndexStoragePaths {

	private final Service service;

	public IndexStoragePaths(Service service) {
		this.service = service;
	}

	/**
	 * @return a mapping between hot/warm db and cold db paths to index name.
	 */
	public Map<String, String> getIndexPaths() {
		EntityCollection<Index> indexes = service.getIndexes();
		if (indexes != null && !indexes.isEmpty())
			return mapIndexesPathsWithIndexName(indexes);
		return Collections.emptyMap();
	}

	private Map<String, String> mapIndexesPathsWithIndexName(
			EntityCollection<Index> indexes) {
		HashMap<String, String> paths = new HashMap<String, String>();
		for (String key : indexes.keySet())
			mapHomeAndColdPathToIndexName(paths, indexes.get(key));
		return paths;
	}

	private void mapHomeAndColdPathToIndexName(HashMap<String, String> paths,
			Index index) {
		mapPathToIndex(paths, index, index.getColdPathExpanded());
		mapPathToIndex(paths, index, index.getHomePathExpanded());
	}

	private void mapPathToIndex(HashMap<String, String> paths, Index index,
			String path) {
		if (path != null)
			paths.put(path, index.getName());
	}
}
