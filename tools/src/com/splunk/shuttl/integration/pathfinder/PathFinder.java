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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
public class PathFinder {

	private PathResolver pathResolver;
	private ArchiveFileSystem fileSystem;

	/**
	 */
	public PathFinder(PathResolver pathResolver, ArchiveFileSystem fileSystem) {
		this.pathResolver = pathResolver;
		this.fileSystem = fileSystem;
	}

	/**
	 * @return
	 * @throws IOException 
	 */
	public List<String> getUnsyncedPaths(List<String> syncedPaths) throws IOException {
		String rootPath = this.pathResolver.getIndexesHome();
		List<String> allPaths = getAllFilePathsFromRootPath(rootPath);
		List<String> unsyncedPaths = removeSyncedPathsFromAllPaths(allPaths,
				syncedPaths);

		return unsyncedPaths;
	}

	public List<String> getAllFilePathsFromRootPath(String rootPath)
			throws IOException {
		List<String> filesPathsInRootPath = new ArrayList<String>();

		if (pathIsFile(rootPath))
			filesPathsInRootPath.add(rootPath);
		else
			filesPathsInRootPath.addAll(getAllFilePathsFromPath(rootPath));

		return filesPathsInRootPath;
	}

	private List<String> getAllFilePathsFromPath(String inputPath)
			throws IOException {
		List<String> filePaths = new ArrayList<String>();
		for (String path : this.fileSystem.listPath(inputPath))
			filePaths.addAll(getAllFilePathsFromRootPath(path));
		return filePaths;
	}

	/**
	 * @param allPaths
	 * @param syncedPaths
	 * @return
	 */
	private List<String> removeSyncedPathsFromAllPaths(List<String> allPaths,
			List<String> syncedPaths) {
		for (String syncedPath : syncedPaths)
			allPaths.remove(syncedPath);
		return allPaths;
	}
	
	/**
	 * @param inputPath
	 * @return
	 */
	private boolean pathIsFile(String inputPath) {
		return inputPath.contains(".csv");
	}

	private List<String> collectAllPathsWithConstraint(
			List<String> pathsToTraverse, String constraint) {
		return pathsToTraverse;
	}

}
