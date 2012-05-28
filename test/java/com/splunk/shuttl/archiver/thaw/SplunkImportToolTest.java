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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.SplunkEnrivonmentNotSetException;
import com.splunk.shuttl.archiver.util.SplunkEnvironment;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

@Test(groups = { "fast-unit" })
public class SplunkImportToolTest {

    private SplunkImportTool splunkImportTool;

    @BeforeMethod
    public void setUp() {
	splunkImportTool = new SplunkImportTool();
    }

    @Test(groups = { "fast-unit" })
    public void getExecutableCommand_theImportToolIsInSplunkHome_pathToExecutable() {
	final File splunkHome = createTempDirectory();
	File bin = createDirectoryInParent(splunkHome, "bin");
	final File importTool = createFileInParent(bin, "importtool");
	TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHome.getAbsolutePath());
		String pathToExecutable = splunkImportTool
			.getExecutableCommand();
		assertEquals(importTool.getAbsolutePath(), pathToExecutable);
	    }
	});
    }

    @Test(expectedExceptions = { SplunkEnrivonmentNotSetException.class })
    public void getExecutableCommand_splunkHomeIsNotSet_throwException() {
	TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		assertTrue(System.getenv("SPLUNK_HOME") == null);
		splunkImportTool.getExecutableCommand();
	    }
	});
    }

    public void getEnvironment_default_getsAllTheSplunkEnvironments() {
	assertEquals(SplunkEnvironment.getEnvironment(),
		splunkImportTool.getEnvironment());
    }
}
