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

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAnyAttribute;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.namespace.QName;

public class ArchiveFormat {

	private Map<QName, String> attributes;

	private String name;

	@XmlAnyAttribute
	public Map<QName, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<QName, String> attributes) {
		this.attributes = attributes;
	}

	@XmlValue
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static ArchiveFormat create(String name, String... kvs) {
		ArchiveFormat f = new ArchiveFormat();
		f.setName(name);
		if (kvs != null)
			f.setAttributes(kvsToAttributes(kvs));
		return f;
	}

	private static HashMap<QName, String> kvsToAttributes(String[] kvs) {
		if (kvs.length % 2 != 0)
			throw new RuntimeException();

		HashMap<QName, String> attrs = new HashMap<QName, String>();
		for (int i = 0; i < kvs.length; i += 2)
			attrs.put(new QName(kvs[i]), kvs[i + 1]);
		return attrs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((attributes == null) ? 0 : attributes.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArchiveFormat other = (ArchiveFormat) obj;
		if (attributes == null) {
			if (other.attributes != null)
				return false;
		} else if (!attributes.equals(other.attributes))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ArchiveFormat [attributes=" + attributes + ", name=" + name + "]";
	}

}