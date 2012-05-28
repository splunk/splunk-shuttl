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
package com.splunk.shuttl.archiver.thaw;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.TUtilsMockito;

@Test(groups = { "fast-unit" })
public class SplunkSettingsTest {

    SplunkSettings splunkSettings;
    Service splunkService;
    private String indexName;
    private String thawLocationPath;

    @BeforeMethod
    public void setUp() {
	indexName = "index";
	thawLocationPath = "/path/to/thaw";
	splunkService = TUtilsMockito
		.createSplunkServiceReturningThawPathForIndex(indexName,
			thawLocationPath);
	splunkSettings = new SplunkSettings(splunkService);
    }

    @Test(groups = { "fast-unit" })
    public void getThawLocation_givenIndexAndSplunkService_getThawDirectoryForIndex()
	    throws IOException {
	File actualThawLocation = splunkSettings.getThawLocation(indexName);
	assertEquals(thawLocationPath, actualThawLocation.getAbsolutePath());
    }

    @Test(groups = { "fast-unit" }, expectedExceptions = { IllegalIndexException.class })
    public void getThawLocation_givenInvalidIndexAndSplunkService_throws()
	    throws IOException {
	String nonexistantIndexName = "asd123";
	File actualThawLocation = splunkSettings
		.getThawLocation(nonexistantIndexName);
	assertEquals(thawLocationPath, actualThawLocation.getAbsolutePath());
    }
}
