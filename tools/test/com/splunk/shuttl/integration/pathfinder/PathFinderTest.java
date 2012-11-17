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
package com.splunk.shuttl.integration.pathfinder;

import static java.util.Arrays.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;

@Test(groups = { "fast-unit" })
public class PathFinderTest {
	private PathFinder pathFinder;
	private PathResolver pathResolver;
	private ArchiveFileSystem fileSystem;

	@BeforeMethod
	public void setUp() throws IOException {
		pathResolver = mock(PathResolver.class);
		fileSystem = mock(ArchiveFileSystem.class);
		this.pathFinder = new PathFinder(pathResolver, fileSystem);

		when(pathResolver.getIndexesHome()).thenReturn("rootPath");
		when(fileSystem.listPath("rootPath")).thenReturn(
						asList("rootPath/file.csv", "rootPath/file2.csv",
						"rootPath/file3.csv"));
	}

	@Test
	public void getAllFilePathsFromRootPath_givenRootPath_returnsAllFilesFromRoot()
			throws IOException {
		List<String> result = pathFinder.getAllFilePathsFromRootPath("rootPath");

		System.out.println(result.get(0));
		assertEquals(3, result.size());
	}

	@Test
	public void getAllFilePathsFromRootPath_complexTreeStructure_returnsAllFiles()
			throws IOException {
		String rootPath = "rootPath";
		String subFolder = rootPath.concat("/subFolder");
		List<String> pathsInRoot = asList(subFolder,
				rootPath.concat("/file.csv"));
		List<String> pathsInSubFolder = asList(subFolder.concat("/file.csv"),subFolder.concat("/file2.csv"));

		when(pathResolver.getIndexesHome()).thenReturn(rootPath);
		when(fileSystem.listPath(rootPath)).thenReturn(pathsInRoot);
		when(fileSystem.listPath(subFolder)).thenReturn(pathsInSubFolder);

		List<String> result = pathFinder.getAllFilePathsFromRootPath("rootPath");

		assertEquals(3, result.size());
	}

	public void getUnsyncedPaths_noSyncedPathsInInput_returnsAllPaths() throws IOException {
		List<String> result = pathFinder.getUnsyncedPaths(new ArrayList<String>());

		assertEquals(3, result.size());
	}

	public void getUnsyncedPaths_oneSyncedPathInInput_returnsAllPathsExceptTheAlreadySynced() throws IOException {
		List<String> inputContainingOnePath = new ArrayList<String>();
		inputContainingOnePath.add("rootPath/file2.csv");
		List<String> result = pathFinder.getUnsyncedPaths(inputContainingOnePath);

		assertEquals(2, result.size());
		assertEquals(true,
				resultDoesNotContainPathToPathsInInput(inputContainingOnePath, result));
	}

	/**
	 * @param inputPaths
	 * @param resultPaths
	 * @return
	 */
	private boolean resultDoesNotContainPathToPathsInInput(
			List<String> inputPaths, List<String> resultPaths) {
		for (String path : inputPaths)
			if (resultPaths.contains(path))
				return false;
		return true;
	}

}
