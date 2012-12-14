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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;

@Test(groups = { "fast-unit" })
public class IndexStoragePathsTest {

	private IndexStoragePaths indexPaths;
	private Service service;

	@BeforeMethod
	public void setUp() {
		service = mock(Service.class);
		indexPaths = new IndexStoragePaths(service);
	}

	private void assertGetIndexPathsReturnsEmptyMap() {
		Map<String, String> paths = indexPaths.getIndexPaths();
		Map<String, String> expected = Collections.emptyMap();
		assertEquals(paths, expected);
	}

	public void getIndexPaths_noIndexes_emptyMap() {
		Map<String, String> paths = indexPaths.getIndexPaths();
		Map<String, String> expected = Collections.emptyMap();
		assertEquals(paths, expected);
	}

	public void getIndexPaths_serviceGetIndexesIsNull_emptyMap() {
		when(service.getIndexes()).thenReturn(null);
		assertGetIndexPathsReturnsEmptyMap();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void getIndexPaths_indexesCollectionIsEmpty_emptyMap() {
		EntityCollection indexes = mock(EntityCollection.class);
		when(service.getIndexes()).thenReturn(indexes);
		when(indexes.isEmpty()).thenReturn(true);
		assertGetIndexPathsReturnsEmptyMap();
		verify(indexes).isEmpty();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void getIndexPaths_indexWithColdPath_mapWithColdPathToIndexName() {
		EntityCollection indexes = mock(EntityCollection.class);
		when(service.getIndexes()).thenReturn(indexes);
		Index index = mock(Index.class);
		String indexName = setupIndexInIndexes(indexes, index);
		File coldDbPath = createFilePath();
		when(index.getColdPathExpanded()).thenReturn(coldDbPath.getAbsolutePath());

		assertPathIsMappedToIndexName(coldDbPath, indexName);
	}

	@SuppressWarnings("rawtypes")
	private String setupIndexInIndexes(EntityCollection indexes, Index index) {
		String indexName = "index";
		when(indexes.keySet()).thenReturn(Sets.newHashSet(indexName));
		when(indexes.get(indexName)).thenReturn(index);
		when(index.getName()).thenReturn(indexName);
		return indexName;
	}

	private void assertPathIsMappedToIndexName(File coldDbPath, String indexName) {
		Map<String, String> paths = indexPaths.getIndexPaths();
		Map<String, String> expected = new HashMap<String, String>();
		expected.put(coldDbPath.getAbsolutePath(), indexName);
		assertEquals(paths, expected);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void getIndexPaths_indexWithHomePath_mapWithHomePathToIndexName() {
		EntityCollection indexes = mock(EntityCollection.class);
		when(service.getIndexes()).thenReturn(indexes);
		Index index = mock(Index.class);
		String indexName = setupIndexInIndexes(indexes, index);
		File homeDbPath = createFilePath();
		when(index.getHomePathExpanded()).thenReturn(homeDbPath.getAbsolutePath());

		assertPathIsMappedToIndexName(homeDbPath, indexName);
	}
}
