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
package com.splunk.shuttl.server.model;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.io.FileUtils;

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Bean for a bucket to respond with rest.
 */
@XmlRootElement(name = "bucket")
public class BucketBean {

	private String format;
	private String indexName;
	private String bucketName;
	private String uri;
	private String from;
	private String to;
	private String size;

	/**
	 * Needed for JAX-RS
	 */
	public BucketBean() {
	}

	public BucketBean(String format, String indexName, String bucketName,
			String uri, String from, String to, String size) {
		this.format = format;
		this.indexName = indexName;
		this.bucketName = bucketName;
		this.uri = uri;
		this.from = from;
		this.to = to;
		this.size = size;
	}

	/**
	 * @return the format
	 */
	public String getFormat() {
		return format;
	}

	/**
	 * @param format
	 *          the format to set
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	/**
	 * @return the indexName
	 */
	public String getIndexName() {
		return indexName;
	}

	/**
	 * @param indexName
	 *          the indexName to set
	 */
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	/**
	 * @return the bucketName
	 */
	public String getBucketName() {
		return bucketName;
	}

	/**
	 * @param bucketName
	 *          the bucketName to set
	 */
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @param uri
	 *          the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getFromDate() {
		return from;
	}

	public String getToDate() {
		return to;
	}

	public void setFromDate(String date) {
		this.from = date;
	}

	public void setToDate(String date) {
		this.to = date;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getSize() {
		return size;
	}

	/**
	 * @param bucket
	 *          to create bean from.
	 */
	public static BucketBean createBeanFromBucket(Bucket bucket) {
		String size = FileUtils.byteCountToDisplaySize(bucket.getSize() == null ? 0
				: bucket.getSize());
		return new BucketBean(bucket.getFormat().name(), bucket.getIndex(),
				bucket.getName(), bucket.getURI().toString(),
				stringFromDate(bucket.getEarliest()),
				stringFromDate(bucket.getLatest()), size);
	}

	private static String stringFromDate(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd").format(date).toString();
	}
}
