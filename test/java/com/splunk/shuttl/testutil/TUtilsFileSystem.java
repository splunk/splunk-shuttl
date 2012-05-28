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

package com.splunk.shuttl.testutil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * All the utils regarding hadoop FileSystem object goes in here. If there are
 * exceptions while doing any operations the tests will fail with appropriate
 * message.
 */
public class TUtilsFileSystem {

    /**
     * Creates a local filesystem failing the test if it can't.
     */
    public static FileSystem getLocalFileSystem() {
	Configuration configuration = new Configuration();
	try {
	    return FileSystem.getLocal(configuration);
	} catch (IOException e) {
	    TUtilsTestNG.failForException("Couldn't create a local filesystem",
		    e);
	    return null; // Will not be executed.
	}
    }

    /**
     * @return The file on specified path from specified filessystem.
     */
    public static File getFileFromFileSystem(FileSystem fileSystem,
	    Path pathOftheFileOnRemote) {
	File retrivedFile = TUtilsFile.createTestFilePath();
	Path localFilePath = new Path(retrivedFile.toURI());
	try {
	    fileSystem.copyToLocalFile(pathOftheFileOnRemote, localFilePath);
	} catch (IOException e) {
	    TUtilsTestNG.failForException(
		    "Can't retrive the file from remote filesystem", e);
	}
	return retrivedFile;
    }
}
