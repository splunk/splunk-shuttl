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
package com.splunk.shep.archiver;

import java.io.File;

import org.apache.commons.io.FileUtils;

/**
 * Constants for creating directories where the Archiver can store its locks,
 * unfinished buckets and other files.
 */
public class LocalFileSystemConstants {

    public static final String ARCHIVER_DIRECTORY_PATH = FileUtils
	    .getUserDirectoryPath() + File.separator + "SplunkArchiverFiles";

    public static final String DEFAULT_SAFE_LOCATION = ARCHIVER_DIRECTORY_PATH
	    + File.separator + "safe-buckets";

    public static final String DEFAULT_FAIL_LOCATION = ARCHIVER_DIRECTORY_PATH
	    + File.separator + "failed-buckets";

    public static final String DEFAULT_LOCKS_DIRECTORY = ARCHIVER_DIRECTORY_PATH
	    + File.separator + "locks-dir";

    public static File getArchiverDirectory() {
	return createDirectory(ARCHIVER_DIRECTORY_PATH);
    }

    private static File createDirectory(String path) {
	File dir = new File(path);
	dir.mkdirs();
	return dir;
    }

    public static File getFailLocation() {
	return createDirectory(DEFAULT_FAIL_LOCATION);
    }

    public static File getSafeLocation() {
	return createDirectory(DEFAULT_SAFE_LOCATION);
    }

    public static File getLocksDirectory() {
	return createDirectory(DEFAULT_LOCKS_DIRECTORY);
    }
}
