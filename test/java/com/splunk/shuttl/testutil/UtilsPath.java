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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * All the utils regarding hadoop Path object goes in here. If there are
 * exceptions while doing any operations the tests will fail with appropriate
 * message.
 */
public class UtilsPath {

    /**
     * Same as calling {@link #getSafeDirectory(FileSystem, Class)} with
     * MethodCallerHelper.getCallerToMyMethod() as the class parameter
     * 
     * @see #getSafeDirectory(FileSystem, Class)
     */
    public static Path getSafeDirectory(FileSystem fileSystem) {
	return getSafeDirectory(fileSystem,
		MethodCallerHelper.getCallerToMyMethod());
    }

    /**
     * SafePathCreator is used to get a directory in a file system which is
     * class unique, readable and writable.
     * 
     * It returns a path like this: /User/XXX/org.shuttl.HadoopTest/
     * 
     */
    public static Path getSafeDirectory(FileSystem fileSystem, Class<?> clazz) {
	return new Path(fileSystem.getHomeDirectory(), clazz.getName());
    }
}