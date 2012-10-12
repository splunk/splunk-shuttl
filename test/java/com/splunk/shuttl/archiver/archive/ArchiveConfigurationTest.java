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
package com.splunk.shuttl.archiver.archive;

import static java.util.Arrays.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;

@Test(groups = { "fast-unit" })
public class ArchiveConfigurationTest {

	private ShuttlArchiverMBean mBean;

	@BeforeMethod
	public void setUp() {
		mBean = mock(ShuttlArchiverMBean.class);
	}

	private ArchiveConfiguration createConfiguration() {
		return ArchiveConfiguration.createConfigurationWithMBean(mBean);
	}

	@Test(groups = { "fast-unit" })
	public void getArchiveFormat_givenAnyFormatAsStringInMBean_returnsBucketFormat() {
		when(mBean.getArchiveFormats()).thenReturn(
				asList(BucketFormat.SPLUNK_BUCKET.name()));
		List<BucketFormat> archiveFormat = createConfiguration()
				.getArchiveFormats();
		assertNotNull(archiveFormat);
	}

	public void getArchiveFormat_givenNull_emptyList() {
		when(mBean.getArchiveFormats()).thenReturn(null);
		assertEquals(new ArrayList<BucketFormat>(), createConfiguration()
				.getArchiveFormats());
	}

	public void getArchiveDataPath_givenPathInMBean_childToThePath() {
		String path = "/archive/path";
		when(mBean.getArchivePath()).thenReturn(path);
		String actualPath = createConfiguration().getArchiveDataPath();
		String childName = FilenameUtils.getName(actualPath);
		String expectedPath = path + "/" + childName;
		assertEquals(expectedPath, actualPath);
	}

	public void getClusterName_stubbedMBeanClusterName_sameAsInMBean() {
		String expected = "clusterName";
		when(mBean.getClusterName()).thenReturn(expected);
		String actual = createConfiguration().getClusterName();
		assertEquals(expected, actual);
	}

	public void getServerName_stubbedMBeanServerName_sameAsInMBean() {
		String expected = "serverName";
		when(mBean.getServerName()).thenReturn(expected);
		String actual = createConfiguration().getServerName();
		assertEquals(expected, actual);
	}

	public void getLocalArchiverDir_stubbedMBeanLocalArchiverDir_sameAsInMBean() {
		String expected = "localArchiverDir";
		when(mBean.getLocalArchiverDir()).thenReturn(expected);
		String actual = createConfiguration().getLocalArchiverDir();
		assertEquals(expected, actual);
	}

	public void getBucketFormatPriority_null_emptyList() {
		when(mBean.getBucketFormatPriority()).thenReturn(null);
		List<BucketFormat> priorityList = createConfiguration()
				.getBucketFormatPriority();
		assertEquals(new ArrayList<BucketFormat>(), priorityList);
	}

	public void getBucketFormatPriority_noFormats_emptyList() {
		when(mBean.getBucketFormatPriority()).thenReturn(new ArrayList<String>());
		List<BucketFormat> priorityList = createConfiguration()
				.getBucketFormatPriority();
		assertEquals(new ArrayList<BucketFormat>(), priorityList);
	}

	public void getBucketFormatPriority_oneFormat_listWithThatOneFormat() {
		List<String> formats = Arrays.asList(BucketFormat.SPLUNK_BUCKET.name());
		when(mBean.getBucketFormatPriority()).thenReturn(formats);
		List<BucketFormat> priorityList = createConfiguration()
				.getBucketFormatPriority();
		assertEquals(1, priorityList.size());
		assertEquals(BucketFormat.SPLUNK_BUCKET, priorityList.get(0));
	}

	public void getBucketFormatPriority_twoFormats_listWithThoseTwoFormats() {
		List<String> formats = Arrays.asList(BucketFormat.SPLUNK_BUCKET.name(),
				BucketFormat.UNKNOWN.name());
		when(mBean.getBucketFormatPriority()).thenReturn(formats);
		List<BucketFormat> priorityList = createConfiguration()
				.getBucketFormatPriority();
		assertEquals(2, priorityList.size());
		assertEquals(BucketFormat.SPLUNK_BUCKET, priorityList.get(0));
		assertEquals(BucketFormat.UNKNOWN, priorityList.get(1));
	}

	public void getArchiveTempPath_givenArchivePath_pathIsAChildOfTheArchivePath() {
		String archivePath = "/archive/path";
		when(mBean.getArchivePath()).thenReturn(archivePath);
		String tempPath = createConfiguration().getArchiveTempPath();

		String tmpDirectoryName = FilenameUtils.getName(tempPath);
		String expected = archivePath + "/" + tmpDirectoryName;
		assertEquals(expected, tempPath);
	}

	public void getArchiveTempPath_givenArchivePath_pathIsNotWithInArchiveDataPath() {
		when(mBean.getArchivePath()).thenReturn("/archive/path");
		ArchiveConfiguration configuration = createConfiguration();
		String archivingRoot = configuration.getArchiveDataPath();
		String tempPath = configuration.getArchiveTempPath();

		assertFalse(tempPath.contains(archivingRoot));
	}

	public void _givenArchivePath_archiveTempPathAndArchiveDataPathHasTheSameParent() {
		when(mBean.getArchivePath()).thenReturn("/archive/path");
		ArchiveConfiguration configuration = createConfiguration();
		String archiveDataPath = configuration.getArchiveDataPath();
		String archiveTempPath = configuration.getArchiveTempPath();

		assertPathsHasSameParent(archiveDataPath, archiveTempPath);
	}

	private void assertPathsHasSameParent(String path1, String path2) {
		assertEquals(new File(path1).getParent(), new File(path2).getParent());
	}
}
