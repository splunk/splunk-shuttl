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

import com.splunk.shuttl.archiver.copy.IndexScanner.UnknownIndexPathException;

@Test(groups = { "fast-unit" })
public class IndexScannerTest {

	private IndexScanner indexScanner;
	private IndexStoragePaths indexPaths;

	@BeforeMethod
	public void setUp() {
		indexPaths = mock(IndexStoragePaths.class);
		indexScanner = new IndexScanner(indexPaths);
	}

	@Test(expectedExceptions = { UnknownIndexPathException.class })
	public void getIndex_givenNoIndexes_throws() {
		Map<String, String> emptyMap = Collections.emptyMap();
		when(indexPaths.getIndexPaths()).thenReturn(emptyMap);
		indexScanner.getIndex(createFile());
	}

	public void getIndex_givenIndexPathMatchesBucketPath_returnsIndexName() {
		File bucketPath = createFile();

		Map<String, String> paths = new HashMap<String, String>();
		String indexName = "index";
		paths.put(bucketPath.getParentFile().getAbsolutePath(), indexName);
		when(indexPaths.getIndexPaths()).thenReturn(paths);

		String actualIndex = indexScanner.getIndex(bucketPath);
		assertEquals(actualIndex, indexName);
	}
}
