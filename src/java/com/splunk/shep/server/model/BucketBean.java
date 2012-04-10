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
package com.splunk.shep.server.model;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Bean for a bucket to respond with rest.
 */
@XmlRootElement(name = "bucket")
public class BucketBean {

    private String format;
    private String indexName;
    private String bucketName;
    private String uri;

    /**
     * Needed for JAX-RS
     */
    public BucketBean() {
	// TODO Auto-generated constructor stub
    }

    public BucketBean(String format, String indexName, String bucketName,
	    String uri) {
	this.format = format;
	this.indexName = indexName;
	this.bucketName = bucketName;
	this.uri = uri;
    }

    /**
     * @return the format
     */
    public String getFormat() {
	return format;
    }

    /**
     * @param format
     *            the format to set
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
     *            the indexName to set
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
     *            the bucketName to set
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
     *            the uri to set
     */
    public void setUri(String uri) {
	this.uri = uri;
    }

}
