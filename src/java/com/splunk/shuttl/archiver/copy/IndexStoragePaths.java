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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
		return mapIndexesPathsWithIndexName(getIndexesChecked());
	}

	private Map<String, Index> getIndexesChecked() {
		EntityCollection<Index> indexes = service.getIndexes();
		if (indexes == null || indexes.isEmpty()) {
			return Collections.emptyMap();
		} else
			return indexes;
	}

	private Map<String, String> mapIndexesPathsWithIndexName(
			Map<String, Index> indexes) {
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

	/**
	 * @return list of paths to where buckets are stored. Namely db, colddb and
	 *         thaweddb.
	 */
	public List<File> getDbPathsForIndex(String indexName) {
		Map<String, Index> indexes = getIndexesChecked();
		if (indexes.containsKey(indexName))
			return listWithDbPaths(indexes.get(indexName));
		else
			return Collections.emptyList();
	}

	private List<File> listWithDbPaths(Index index) {
		List<File> paths = new ArrayList<File>();
		addPathIfNotNull(paths, index.getHomePathExpanded());
		addPathIfNotNull(paths, index.getColdPathExpanded());
		addPathIfNotNull(paths, index.getThawedPathExpanded());
		return paths;
	}

	private void addPathIfNotNull(List<File> paths, String path) {
		if (path != null)
			paths.add(new File(path));
	}
}
