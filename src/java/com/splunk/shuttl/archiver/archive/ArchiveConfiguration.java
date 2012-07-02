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

package com.splunk.shuttl.archiver.archive;

import java.lang.ref.SoftReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;

public class ArchiveConfiguration {

	private final BucketFormat bucketFormat;
	private final URI archivingRoot;
	private final String clusterName;
	private final String serverName;
	private final List<BucketFormat> bucketFormatPriority;
	private final URI tmpDirectory;

	public ArchiveConfiguration(BucketFormat bucketFormat, URI archivingRoot,
			String clusterName, String serverName,
			List<BucketFormat> bucketFormatPriority, URI tmpDirectory) {
		this.bucketFormat = bucketFormat;
		this.archivingRoot = archivingRoot;
		this.clusterName = clusterName;
		this.serverName = serverName;
		this.bucketFormatPriority = bucketFormatPriority;
		this.tmpDirectory = tmpDirectory;
	}

	/**
	 * Soft link so the memory can be used if needed. (Soft links are
	 * GarbageCollected only if there is really need for the memory)
	 */
	private static SoftReference<ArchiveConfiguration> sharedInstanceRef;

	public static ArchiveConfiguration getSharedInstance() {
		ArchiveConfiguration sharedInstance = null;
		if (sharedInstanceRef != null)
			sharedInstance = sharedInstanceRef.get();

		if (sharedInstance == null) {
			sharedInstance = createConfigurationWithMBean(ShuttlArchiver
					.getMBeanProxy());
			sharedInstanceRef = new SoftReference<ArchiveConfiguration>(
					sharedInstance);
		}
		return sharedInstance;
	}

	/**
	 * @return {@link ArchiveConfiguration} with properties from a
	 *         {@link ShuttlArchiverMBean}
	 */
	public static ArchiveConfiguration createConfigurationWithMBean(
			ShuttlArchiverMBean mBean) {
		BucketFormat bucketFormat = bucketFormatFromMBean(mBean);
		URI archivingRoot = archivingRootFromMBean(mBean);
		String clusterName = mBean.getClusterName();
		String serverName = mBean.getServerName();
		List<BucketFormat> bucketFormatPriority = createFormatPriorityList(mBean);
		URI tmpDirectory = getTmpDirectoryFromArchivingRoot(mBean, archivingRoot);
		return new ArchiveConfiguration(bucketFormat, archivingRoot, clusterName,
				serverName, bucketFormatPriority, tmpDirectory);
	}

	private static URI archivingRootFromMBean(ShuttlArchiverMBean mBean) {
		String archivingRoot = mBean.getArchiverRootURI();
		return archivingRoot != null ? URI.create(archivingRoot) : null;
	}

	private static BucketFormat bucketFormatFromMBean(ShuttlArchiverMBean mBean) {
		String archiveFormat = mBean.getArchiveFormat();
		return archiveFormat != null ? BucketFormat.valueOf(archiveFormat) : null;
	}

	private static URI getTmpDirectoryFromArchivingRoot(ShuttlArchiverMBean mBean, URI archivingRoot) {
		String tmpDir = mBean.getTmpDirectory();
		return tmpDir != null ? archivingRoot.resolve(tmpDir) : null;
	}

	private static List<BucketFormat> createFormatPriorityList(
			ShuttlArchiverMBean mBean) {
		List<BucketFormat> bucketFormats = new ArrayList<BucketFormat>();
		for (String format : mBean.getBucketFormatPriority())
			bucketFormats.add(BucketFormat.valueOf(format));
		return bucketFormats;
	}

	public BucketFormat getArchiveFormat() {
		return bucketFormat;
	}

	public URI getArchivingRoot() {
		return archivingRoot;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getServerName() {
		return serverName;
	}

	/**
	 * List of bucket formats, where lower index means it has higher priority. <br/>
	 * {@link ArchiveConfiguration#getBucketFormatPriority()}.get(0) has the
	 * highest priority, while .get(length-1) has the least priority.
	 */
	public List<BucketFormat> getBucketFormatPriority() {
		return bucketFormatPriority;
	}

	/**
	 * @return The Path on hadoop filesystem that is used as a temp directory
	 */
	public URI getTmpDirectory() {
		return tmpDirectory;
	}

}
