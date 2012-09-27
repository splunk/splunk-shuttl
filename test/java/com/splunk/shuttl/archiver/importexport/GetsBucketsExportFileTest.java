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
package com.splunk.shuttl.archiver.importexport;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class GetsBucketsExportFileTest {

	private File dir;
	private GetsBucketsExportFile getsBucketsExportFile;
	private Bucket bucket;
	private String extension;

	@BeforeMethod
	public void setUp() {
		bucket = TUtilsBucket.createBucket();
		dir = createDirectory();
		extension = "ext";
		getsBucketsExportFile = new GetsBucketsExportFile(dir);
	}

	public void __fileInDirWithIndexName() {
		File file = getsBucketsExportFile.getExportFile(bucket, extension);
		assertEquals(bucket.getIndex(), file.getParentFile().getName());
	}

	public void __parentDirectoryExists() {
		File file = getsBucketsExportFile.getExportFile(bucket, extension);
		assertTrue(file.getParentFile().exists());
	}

	public void __parentsParentIsTheDirGivenToTheConstructor() {
		File file = getsBucketsExportFile.getExportFile(bucket, extension);
		assertEquals(dir.getAbsolutePath(), file.getParentFile().getParentFile()
				.getAbsolutePath());
	}

	public void __fileEndsWithExtension() {
		File file = getsBucketsExportFile.getExportFile(bucket, extension);
		assertTrue(file.getName().endsWith(extension));
	}

	public void _fileAlreadyExists_deletesIt() throws IOException {
		File file = getsBucketsExportFile.getExportFile(bucket, extension);
		assertTrue(file.createNewFile());
		assertTrue(file.exists());
		getsBucketsExportFile.getExportFile(bucket, extension);
		assertFalse(file.exists());
	}

	public void __fileHasBucketNameForUniquness() {
		File file = getsBucketsExportFile.getExportFile(bucket, extension);
		assertEquals(bucket.getName() + "." + extension, file.getName());
	}
}
