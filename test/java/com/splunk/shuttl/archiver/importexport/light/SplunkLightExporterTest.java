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
import static org.mockito.Mockito.*;
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

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "fast-unit" })
public class SplunkLightExporterTest {

	private BucketFormat format = SplunkLightExporter.EXPORT_FORMAT;
	private LocalBucket bucket;
	private LocalFileSystemPaths localFileSystemPaths;
	private File exportDirectory;

	@BeforeMethod
	public void setUp() throws IOException {
		exportDirectory = TUtilsFile.createDirectory();
		bucket = TUtilsBucket.createBucket();
		cleanBucketDir(bucket);
		createDirs(rawdata(bucket));
		createFile(fooFile(bucket));
		createFile(journalGz(bucket));
		localFileSystemPaths = mock(LocalFileSystemPaths.class);
		when(localFileSystemPaths.getExportDirectory(bucket)).thenReturn(
				exportDirectory);
	}

	private File journalGz(LocalBucket bucket) {
		return new File(bucket.getDirectory(), "rawdata/journal.gz");
	}

	private File fooFile(LocalBucket bucket) {
		return new File(bucket.getDirectory(), "foo.file");
	}

	private File rawdata(LocalBucket bucket) {
		return new File(bucket.getDirectory(), "rawdata");
	}

	public void exportBucket_whitelistingRawdata_exportsRawdataDirWithContents()
			throws IOException {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(SplunkLightExporter.EXPORT_FORMAT)
				.put("whitelist", "rawdat.*");

		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);
		Assert.assertNotEquals(exported.getDirectory(), bucket.getDirectory());
		assertTrue(rawdata(exported).exists());
		assertTrue(journalGz(exported).exists());
		assertFalse(fooFile(exported).exists());
	}

	public void exportBucket_blacklistJournal_doesNotExportTheRawdataDir() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("blacklist", ".*journal.gz");

		LocalBucket export = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);

		assertFalse(journalGz(export).exists());
		assertFalse(rawdata(export).exists());
		assertTrue(fooFile(export).exists());
	}

	public void exportBucket_noMetadata_exportsAllFiles() {
		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				getLazyMetadata()).exportBucket(bucket);
		assertTrue(journalGz(exported).exists());
		assertTrue(rawdata(exported).exists());
		assertTrue(fooFile(exported).exists());
	}

	public void exportBucket_whiteAndBlacklist_blacklistingTrumpsWhitelisting() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", "rawdata.*,foo.*");
		metadata.get(format).put("blacklist", ".*journal.gz");

		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);
		assertFalse(journalGz(exported).exists());
		assertFalse(rawdata(exported).exists());
		assertTrue(fooFile(exported).exists());
	}

	public void exportBucket_whitelistFileWithinADirectory_exportsDirectoryToo() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", ".*journal.gz");

		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);
		assertTrue(journalGz(exported).exists());
		assertTrue(rawdata(exported).exists());
		assertFalse(fooFile(exported).exists());
	}

	public void exportBucket_whitelistCanListMultipleFiles_exportsAllListedFiles() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", ".*journal.gz,foo.*");

		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);
		assertTrue(journalGz(exported).exists());
		assertTrue(rawdata(exported).exists());
		assertTrue(fooFile(exported).exists());
	}

	public void exportBucket_blacklistCanListMutlipleFiles_doesNotExportBlacklistedFiles() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("blacklist", "foo.*,.*journal.*");

		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);
		assertFalse(journalGz(exported).exists());
		assertFalse(rawdata(exported).exists());
		assertFalse(fooFile(exported).exists());
	}

	public void exportBucket_whitelistFilename_exportsFilename() {
		Map<BucketFormat, Map<String, String>> metadata = getLazyMetadata();
		metadata.get(format).put("whitelist", "journal.gz");

		LocalBucket exported = SplunkLightExporter.create(localFileSystemPaths,
				metadata).exportBucket(bucket);
		assertTrue(journalGz(exported).exists());
		assertTrue(rawdata(exported).exists());
		assertFalse(fooFile(exported).exists());
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

			validateNewFile(bucketDir, "file");
			validateNewDirs(bucketDir, "dir/dir2/dir3");
			validateNewFile(bucketDir, "dir/file");
			validateNewFile(bucketDir, "dir/dir2/dir3/file");
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

	private void validateNewDirs(File bucketDir, String child) {
		assertTrue(new File(bucketDir, child).mkdirs());
	}

	private void validateNewFile(File bucketDir, String child) throws IOException {
		assertTrue(new File(bucketDir, child).createNewFile());

	}

	private void createDirs(File d) {
		assertTrue(d.mkdirs());
	}

	private void createFile(File f) throws IOException {
		assertTrue(f.createNewFile());
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
