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

import static org.testng.AssertJUnit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class PathFinderTest {
	private PathFinder pathFinder;

	@BeforeMethod
	public void setUp() {
		this.pathFinder = new PathFinder();
	}
	public void getUnsyncedPaths_noSyncedPathsInInput_returnsAllPaths() {
		List<String> result = pathFinder.getUnsyncedPaths(new ArrayList<String>());

		assertEquals(result.size(), 5);
	}

	public void getUnsyncedPaths_oneSyncedPathInInput_returnsAllPathsExceptTheAlreadySynced() {
		List<String> inputContainingOnePath = Arrays.asList("path2");
		List<String> result = pathFinder.getUnsyncedPaths(inputContainingOnePath);

		assertEquals(4, result.size());
		assertEquals(true,
				resultDoesNotContainPathToPathsInInput(inputContainingOnePath, result));
	}

	/**
	 * @param inputPaths
	 * @param resultPaths
	 * @return
	 */
	private Object resultDoesNotContainPathToPathsInInput(
			List<String> inputPaths, List<String> resultPaths) {
		for (String path : inputPaths)
			if (resultPaths.contains(path))
				return false;
		return true;
	}
}
