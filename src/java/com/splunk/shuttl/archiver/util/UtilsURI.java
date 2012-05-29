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

import java.io.File;
import java.net.URI;

import org.apache.commons.io.FilenameUtils;

/**
 * Utils for {@link URI}.
 */
public class UtilsURI {

	/**
	 * Trim eventual ending {@link File#separator}.<br/>
	 * Ex:<br/>
	 * 
	 * <pre>
	 * "file:/a/b/c" -> "/a/b/c"
	 * "file:/a/b/c/" -> "/a/b/c"
	 * "file:/a/b/c.txt" -> "/a/b/c.txt"
	 * </pre>
	 */
	public static String getPathByTrimmingEndingFileSeparator(URI uri) {
		String path = uri.getPath();
		if (path.endsWith(File.separator))
			return path.substring(0, path.length() - 1);
		else
			return path;
	}

	/**
	 * Trim eventual ending {@link File#separator} and return base name.<br/>
	 * Ex:<br/>
	 * 
	 * <pre>
	 * "file:/a/b/c" -> "c"
	 * "file:/a/b/c/" -> "c"
	 * "file:/a/b/c.txt" -> "c.txt"
	 * </pre>
	 */
	public static String getFileNameWithTrimmedEndingFileSeparator(URI uri) {
		String path = getPathByTrimmingEndingFileSeparator(uri);
		return FilenameUtils.getName(path);
	}

}
