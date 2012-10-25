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
package com.splunk.shuttl.archiver.metastore;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.metastore.FlatFileStorage.FlatFileReadException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class FlatFileStorageTest {

	FlatFileStorage flatFileStorage;
	LocalFileSystemPaths real;
	LocalBucket bucket;
	String fileName;

	@BeforeMethod
	public void setUp() {
		real = new LocalFileSystemPaths(createDirectory());
		flatFileStorage = new FlatFileStorage(real);

		bucket = TUtilsBucket.createBucket();
		fileName = "fileName";
	}

	public void getFlatFile_bucket_fileLivesInMetadataDirectoryForBucket() {
		File flatFile = flatFileStorage.getFlatFile(bucket, fileName);
		assertEquals(flatFile.getParentFile(), real.getMetadataDirectory(bucket));
	}

	public void getFlatFile_fileName_hasFileName() {
		assertEquals(fileName, flatFileStorage.getFlatFile(bucket, fileName)
				.getName());
	}

	public void writeFlatFile_bucketFileNameAndData_writesDataToFlatFileIdentifiedToBucketAndFileName() {
		Long data = 123L;
		flatFileStorage.writeFlatFile(bucket, fileName, data);
		Long actualData = flatFileStorage.readFlatFile(flatFileStorage.getFlatFile(
				bucket, fileName));
		assertEquals(data, actualData);
	}

	@Test(expectedExceptions = { FlatFileReadException.class })
	public void readFlatFile_givenEmptyFile_throws() {
		flatFileStorage.readFlatFile(createFile());
	}
}
