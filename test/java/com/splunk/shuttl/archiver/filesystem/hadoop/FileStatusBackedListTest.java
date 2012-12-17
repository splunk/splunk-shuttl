// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.filesystem.hadoop;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class FileStatusBackedListTest {

	private FileStatusBackedList pathList;
	private FileStatus[] fileStatus;

	@BeforeMethod
	public void beforeMethod() {
		fileStatus = new FileStatus[] { mock(FileStatus.class),
				mock(FileStatus.class) };
		pathList = new FileStatusBackedList(fileStatus);
	}

	@Test(groups = { "fast-unit" })
	public void FileStatusBackedList() {
		assertNotNull(pathList);
	}

	public void get_correctIndex_correctItem() throws URISyntaxException {
		URI uri0 = new URI("file:///path/to/a/file");
		URI uri1 = new URI("file:///path/to/an/other/file");
		when(fileStatus[0].getPath()).thenReturn(new Path(uri0));
		when(fileStatus[1].getPath()).thenReturn(new Path(uri1));

		// Test
		assertEquals(uri0.getPath(), pathList.get(0));
		assertEquals(uri1.getPath(), pathList.get(1));

	}

	public void size_emptyList_returnZero() {
		assertEquals(0, (new FileStatusBackedList(new FileStatus[0])).size());
	}

	public void size_nonEmpty_returnCorrectSize() {
		assertEquals(2, pathList.size());
	}
}
