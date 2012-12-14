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
package com.splunk.shuttl.archiver.usecases.util;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class FakeSplunkIndexesLayerTest {

	private File thawLocation;
	private FakeSplunkIndexesLayer fake;

	@BeforeMethod
	public void setUp() {
		thawLocation = createDirectory();
		fake = new FakeSplunkIndexesLayer(thawLocation);
	}

	public void getThawLocation_file_returnsFile() {
		File actualThawLocation = fake.getThawLocation("foo");
		assertEquals(thawLocation.getAbsolutePath(),
				actualThawLocation.getAbsolutePath());
	}

	public void getIndexes_askingForThawLocationInAnyIndex_getsThawLocation() {
		String thawedPathExpanded = fake.getIndexes().get("someIndex")
				.getThawedPathExpanded();
		assertEquals(thawedPathExpanded, thawLocation.getAbsolutePath());
	}
}
