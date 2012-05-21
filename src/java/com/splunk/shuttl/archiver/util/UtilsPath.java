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
package com.splunk.shuttl.archiver.util;

import org.apache.hadoop.fs.Path;

/**
 * Utils for {@link Path}.
 */
public class UtilsPath {

    /**
     * When appending the scheme is taking from pathToAppend and only the actual
     * path is taking from pathThatWillBeAppended
     * 
     * @param pathThatWillBeAppended
     *            This is the base path the scheme will be taken from this one.
     * @param pathToAppend
     *            The path string is taken from this argument and appended to
     *            the previous one.
     * 
     * @return a new Path created by appending 'pathToAppend' to
     *         'pathThatWillBeAppended'
     */
    public static Path createPathByAppending(Path pathThatWillBeAppended,
	    Path pathToAppend) {
	return pathThatWillBeAppended.suffix(pathToAppend.toUri().getPath());
    }

}
