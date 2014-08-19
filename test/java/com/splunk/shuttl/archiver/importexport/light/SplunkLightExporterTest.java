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
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "fast-unit" })
public class SplunkLightExporterTest {

	private BucketFormat format = SplunkLightExporter.EXPORT_FORMAT;
	private LocalBucket bucket;
	private File cleanBucketDir;
	private File rawdata;
	private File fooFile;
	private File journalgz;

	@BeforeMethod
	public void setUp() throws IOException {
		bucket = TUtilsBucket.createBucket();
		cleanBucketDir = cleanBucketDir(bucket);
		rawdata = createDirs(cleanBucketDir, "rawdata");
		fooFile = createFile(cleanBucketDir, "foo.file");
		journalgz = createFile(cleanBucketDir, "rawdata/journal.gz");
	}

	public void exportBucket_whitelistingRawdata_keepsRawdataDirWithContents()
			throws IOException {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(SplunkLightExporter.EXPORT_FORMAT)
				.put("whitelist", "rawdat.*");

		LocalBucket exportedBucket = SplunkLightExporter.create(metadata)
				.exportBucket(bucket);
		Assert.assertEquals(exportedBucket.getDirectory(), bucket.getDirectory());
		assertTrue(rawdata.exists());
		assertTrue(journalgz.exists());
		assertFalse(fooFile.exists());
	}

	public void exportBucket_blacklistDir_removesDir() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("blacklist", ".*journal.gz");

		SplunkLightExporter.create(metadata).exportBucket(bucket);
		assertFalse(journalgz.exists());
		assertTrue(rawdata.exists());
		assertTrue(fooFile.exists());
	}

	public void exportBucket_noMetadata_keepsAllFiles() {
		SplunkLightExporter.create(getLazyMetadata()).exportBucket(bucket);
		assertTrue(journalgz.exists());
		assertTrue(rawdata.exists());
		assertTrue(rawdata.exists());
	}

	public void exportBucket_whiteAndBlacklist_blacklistingTrumpsWhitelisting() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", "rawdata.*");
		metadata.get(format).put("blacklist", ".*journal.gz");

		SplunkLightExporter.create(metadata).exportBucket(bucket);
		assertFalse(journalgz.exists());
		assertTrue(rawdata.exists());
		assertFalse(fooFile.exists()); // does not mention fooFile
	}

	public void exportBucket_whitelistFileWithinADirectory_keepsDirectoryToo() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", ".*journal.gz");

		SplunkLightExporter.create(metadata).exportBucket(bucket);
		assertTrue(journalgz.exists());
		assertTrue(rawdata.exists());
		assertFalse(fooFile.exists());
	}

	public void exportBucket_whitelistCanListMultipleFiles_keepsAllListedFiles() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", "rawdata,foo.*");

		SplunkLightExporter.create(metadata).exportBucket(bucket);
		assertFalse(journalgz.exists());
		assertTrue(rawdata.exists());
		assertTrue(fooFile.exists());
	}

	public void exportBucket_blacklistCanListMutlipleFiles_deletesAllListedFiles() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("blacklist", "foo.*,.*journal.*");

		SplunkLightExporter.create(metadata).exportBucket(bucket);
		assertFalse(journalgz.exists());
		assertTrue(rawdata.exists());
		assertFalse(fooFile.exists());
	}

	public void exportBucket_whitelistFilename_keepsFilename() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", journalgz.getName());

		SplunkLightExporter.create(metadata).exportBucket(bucket);
		assertTrue(journalgz.exists());
		assertTrue(rawdata.exists());
		assertFalse(fooFile.exists());
	}

	public void getPatterns_givenMetadataPerFormat_groupsPatternsByFormat() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", ".*");
		metadata.get(format).put("blacklist", "");

		assertEquals(SplunkLightExporter.getPatterns(metadata, "whitelist", "foo")
				.get(format), getPatternList(".*"));
		assertEquals(SplunkLightExporter.getPatterns(metadata, "blacklist", "foo")
				.get(format), getPatternList(""));
		assertEquals(SplunkLightExporter.getPatterns(metadata, "no-list", "foo")
				.get(format), getPatternList("foo"));
	}

	public void getPatterns_formatWithoutMetadata_hasDefaultPattern() {
		Map<BucketFormat, Map<String, String>> emptyMetadata = getLazyMetadata();

		Map<BucketFormat, List<Pattern>> patterns = SplunkLightExporter
				.getPatterns(emptyMetadata, "metaDataDoesntHaveThisKey",
						"defaultPattern");
		assertEquals(patterns.get(format), getPatternList("defaultPattern"));
	}

	public void getPatterns_canContainMultiplePatterns_getsAllPatterns() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", "foo.*,.*bar");

		assertEquals(SplunkLightExporter.getPatterns(metadata, "whitelist", "nope")
				.get(format), getPatternList("foo.*", ".*bar"));
	}

	private List<Pattern> getPatternList(String... patterns) {
		List<Pattern> ps = new ArrayList<Pattern>();
		for (String pattern : patterns)
			ps.add(Pattern.compile(pattern));
		return ps;
	}

	public void getPathsRelativeToBucket_givenBucketWithFilesAndDirs_getsPathsRelativeToBucket()
			throws IOException {
		File testDir = TUtilsFile.createDirectory();
		try {
			LocalBucket b = TUtilsBucket.createBucketInDirectory(testDir);
			// Clean bucket
			File bucketDir = cleanBucketDir(b);

			createFile(bucketDir, "file");
			createDirs(bucketDir, "dir/dir2/dir3");
			createFile(bucketDir, "dir/file");
			createFile(bucketDir, "dir/dir2/dir3/file");
			Set<String> expectedKeySet = new HashSet<String>();
			expectedKeySet.addAll(asList("file", "dir/file", "dir/dir2/dir3/file"));

			HashMap<String, File> pathsRelativeToBucket = SplunkLightExporter
					.getFilesRelativeToBucket(b, bucketDir);
			Assert.assertEquals(pathsRelativeToBucket.keySet(), expectedKeySet);
			for (Entry<String, File> e : pathsRelativeToBucket.entrySet()) {
				assertTrue(e.getValue().getAbsolutePath().endsWith(e.getKey()));
			}
		} finally {
			FileUtils.deleteQuietly(testDir);
		}
	}

	private File createDirs(File bucketDir, String path) {
		File dir = new File(bucketDir, path);
		assertTrue(dir.mkdirs());
		return dir;
	}

	private File createFile(File bucketDir, String path) throws IOException {
		File file = new File(bucketDir, path);
		assertTrue(file.createNewFile());
		return file;
	}

	private File cleanBucketDir(LocalBucket b) {
		File dir = b.getDirectory();
		FileUtils.deleteQuietly(dir);
		assertTrue(dir.mkdirs());
		return dir;
	}

	private Map<BucketFormat, Map<String, String>> getLazyMetadata() {
		return new HashMap<BucketFormat, Map<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, String> get(Object key) {
				if (!containsKey(key))
					put((BucketFormat) key, new HashMap<String, String>());
				return super.get(key);
			}
		};
	}

	private void assertEquals(List<Pattern> ps1, List<Pattern> ps2) {
		for (Iterator<Pattern> it1 = ps1.iterator(), it2 = ps2.iterator(); it1
				.hasNext() || it2.hasNext();) {
			Assert.assertEquals(it1.next().pattern(), it2.next().pattern());
		}
	}
}
