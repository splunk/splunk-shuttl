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
package com.splunk.shuttl.archiver.importexport.light;

import static java.util.Arrays.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Keeps some of the files of a bucket, defined by whitelisting and blacklisting
 * a format's metadata.
 */
public class SplunkLightExporter implements BucketExporter {

	private static final String MATCH_NOTHING_PATTERN = "";
	private static final String MATCH_ALL_PATTERN = ".*";

	private final Map<BucketFormat, List<Pattern>> whitelists;
	private final Map<BucketFormat, List<Pattern>> blacklists;

	public SplunkLightExporter(Map<BucketFormat, Map<String, String>> metadata) {
		this.whitelists = getWhitelists(metadata);
		this.blacklists = getBlacklists(metadata);
	}

	@Override
	public LocalBucket exportBucket(LocalBucket b) {
		File directory = b.getDirectory();
		for (Entry<String, File> e : getPathsRelativeToBucket(b, directory)
				.entrySet()) {
			if (!shouldKeepPath(e.getKey(), b.getFormat())) {
				FileUtils.deleteQuietly(e.getValue());
			}
		}
		return b;
	}

	private boolean shouldKeepPath(String path, BucketFormat format) {
		boolean whitelisted = isPathMatching(whitelists, path, format);
		boolean blacklisted = isPathMatching(blacklists, path, format);
		return whitelisted && !blacklisted;
	}

	private boolean isPathMatching(Map<BucketFormat, List<Pattern>> patterns,
			String path, BucketFormat format) {
		for (Pattern pattern : patterns.get(format)) {
			if (pattern.matcher(path).matches()) {
				return true;
			}
		}
		return false;
	}

	private Map<BucketFormat, List<Pattern>> getBlacklists(
			Map<BucketFormat, Map<String, String>> metadata) {
		return getPatterns(metadata, "blacklist", MATCH_NOTHING_PATTERN);
	}

	private Map<BucketFormat, List<Pattern>> getWhitelists(
			Map<BucketFormat, Map<String, String>> metadata) {
		return getPatterns(metadata, "whitelist", MATCH_ALL_PATTERN);
	}

	/**
	 * For every format, return patterns which should be matched by specified key.
	 * Where key is either whitelist or blacklist.
	 */
	public static Map<BucketFormat, List<Pattern>> getPatterns(
			Map<BucketFormat, Map<String, String>> metadata, String key,
			String defaultPattern) {
		Map<BucketFormat, List<Pattern>> patterns = new HashMap<BucketFormat, List<Pattern>>();

		Pattern defPattern = Pattern.compile(defaultPattern);
		for (BucketFormat format : BucketFormat.values()) {
			if (metadata.containsKey(format)) {
				Map<String, String> meta = metadata.get(format);
				patterns.put(format, getPattern(meta, key, defPattern));
			} else {
				patterns.put(format, asList(defPattern));
			}
		}
		return patterns;
	}

	private static List<Pattern> getPattern(Map<String, String> meta, String patternKey,
			Pattern defaultPattern) {
		if (!meta.containsKey(patternKey))
			return asList(defaultPattern);

		String listOfPatterns = meta.get(patternKey);
		ArrayList<Pattern> patterns = new ArrayList<Pattern>();
		for (String pattern : listOfPatterns.split(",")) {
			patterns.add(Pattern.compile(pattern));
		}
		return patterns;
	}

	/**
	 * Get paths in a directory, relative to a bucket.
	 * 
	 * <pre>
	 * Example:
	 * bucket_dir/file
	 * bucket_dir/dir/file
	 * 
	 * Will return:
	 * {"file"     : new File(bucket_dir, "file"),
	 *  "dir"      : new File(bucket_dir, "dir"),
	 *  "dir/file" : new File(bucket_dir, "dir/file")}
	 * </pre>
	 */
	public static HashMap<String, File> getPathsRelativeToBucket(LocalBucket b,
			File currentDir) {
		HashMap<String, File> pathToFile = new HashMap<String, File>();
		String pathToBucket = b.getDirectory().getAbsolutePath();
		for (File f : currentDir.listFiles()) {
			pathToFile.put(getPathRelativeToBucket(pathToBucket, f), f);
			if (f.isDirectory()) {
				pathToFile.putAll(getPathsRelativeToBucket(b, f));
			}
		}
		return pathToFile;
	}

	private static String getPathRelativeToBucket(String pathToBucket, File f) {
		String removeBucketPath = StringUtils.removeStart(f.getAbsolutePath(),
				pathToBucket);
		return StringUtils.substringAfter(removeBucketPath, File.separator);
	}

	public static SplunkLightExporter create(ArchiveConfiguration config) {
		Map<BucketFormat, Map<String, String>> metadata = config
				.getFormatMetadata();
		return new SplunkLightExporter(metadata);
	}

}
