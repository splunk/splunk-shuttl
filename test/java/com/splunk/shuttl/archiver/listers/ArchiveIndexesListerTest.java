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
package com.splunk.shuttl.archiver.listers;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;

@Test(groups = { "fast-unit" })
public class ArchiveIndexesListerTest {

	PathResolver pathResolver;
	ArchivedIndexesLister archivedIndexesLister;
	ArchiveFileSystem fileSystem;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		pathResolver = mock(PathResolver.class);
		fileSystem = mock(ArchiveFileSystem.class);
		archivedIndexesLister = new ArchivedIndexesLister(pathResolver, fileSystem);
	}

	@Test(groups = { "fast-unit" })
	public void listIndexes_givenPathResolver_usePathResolverToGetWhereIndexesLive() {
		archivedIndexesLister.listIndexes();
		verify(pathResolver).getIndexesHome();
	}

	public void listIndexes_givenPathResolverAndArchiveFileSystem_useIndexesHomeToListArchivedIndexes()
			throws IOException {
		String path = "/path/indexesHome";
		when(pathResolver.getIndexesHome()).thenReturn(path);
		archivedIndexesLister.listIndexes();
		verify(fileSystem).listPath(path);
	}

	public void listIndexes_givenDirectoriesWhoseNamesAreIndexesFromFileSystem_getIndexes()
			throws IOException {
		String index1 = "dir1";
		String index2 = "dir2";
		List<String> pathsToIndexDirectories = Arrays.asList(
				"/valid/path/" + index1, "/valid/path/" + index2);
		when(fileSystem.listPath(anyString())).thenReturn(pathsToIndexDirectories);
		List<String> listIndexes = archivedIndexesLister.listIndexes();
		assertEquals(listIndexes, Arrays.asList(index1, index2));
	}

}
