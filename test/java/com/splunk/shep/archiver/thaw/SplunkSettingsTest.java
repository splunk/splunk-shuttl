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
package com.splunk.shep.archiver.thaw;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;

@Test(groups = { "fast" })
public class SplunkSettingsTest {

    SplunkSettings splunkSettings;
    Service splunkService;

    @BeforeMethod
    public void setUp() {
	splunkService = mock(Service.class);
	splunkSettings = new SplunkSettings(splunkService);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = { "fast" })
    public void getThawLocation_givenIndexAndSplunkService_getThawDirectoryForIndex() {
	String indexName = "index";
	String thawLocationPath = "/path/to/thaw";
	EntityCollection<Index> indexesMock = mock(EntityCollection.class);
	Index indexMock = mock(Index.class);

	when(splunkService.getIndexes()).thenReturn(indexesMock);
	when(indexesMock.get(indexName)).thenReturn(indexMock);
	when(indexMock.getThawedPathExpanded()).thenReturn(thawLocationPath);

	// Test
	File actualThawLocation = splunkSettings.getThawLocation(indexName);
	assertEquals(thawLocationPath, actualThawLocation.getAbsolutePath());
    }
}
