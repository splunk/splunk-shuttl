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

import static java.util.Arrays.*;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

@Test(groups = { "fast-unit" })
public class JsonUtilsTest {

	public void merge_jsonWithSameKeyDifferentValues_jsonArrayWithBothValues()
			throws JSONException {
		JSONObject o1 = json("{ \"k\" : 1 }");
		JSONObject o2 = json("{ \"k\" : 2 }");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2), "k");
		assertEquals(merge.get("k").toString(), "[1,2]");
		assertEquals("{\"k\":[1,2]}", merge.toString());
	}

	public void merge_identicalJsons_addsBothValues() throws JSONException {
		JSONObject o1 = json("{ \"k\" : 1 }");
		JSONObject o2 = json("{ \"k\" : 1 }");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2), "k");
		assertEquals(merge.get("k").toString(), "[1,1]");
	}

	public void merge_jsonWithDifferentKeys_otherKeyDoesNotExist()
			throws JSONException {
		JSONObject o1 = json("{ \"k\" : 1 }");
		JSONObject o2 = json("{ \"k\" : 1, \"j\" : 2 }");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2), "k");
		assertEquals(merge.get("k").toString(), "[1,1]");
		try {
			merge.get("j");
			fail();
		} catch (JSONException e) {
		}
	}

	public void merge_jsonWithArrays_addsAllValuesToTheSameArray()
			throws JSONException {
		JSONObject o1 = json("{ \"k\" : [1,1] }");
		JSONObject o2 = json("{ \"k\" : [2,2] }");
		JSONObject o3 = json("{ \"k\" : 3 }");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2, o3), "k");
		assertEquals(merge.get("k").toString(), "[1,1,2,2,3]");
	}

	public void merge_oneEmptyJson_equalsNonEmptyOne() throws JSONException {
		JSONObject o1 = json("{ \"k\" : 1 }");
		JSONObject o2 = json("{ }");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2), "k");
		assertEquals(merge.get("k").toString(), "1");
	}

	public void merge_keyWithEmptyCollection_returnsSameJson() {
		JSONObject o1 = json("{\"k\":[]}");
		JSONObject merge = JsonUtils.mergeKey(asList(o1), "k");
		assertEquals(o1.toString(), merge.toString());
	}

	public void merge_oneEmptyAndOneNotEmptyCollection_notEmptyCollection() {
		JSONObject o1 = json("{\"k\":[]}");
		JSONObject o2 = json("{\"k\":[1]}");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2), "k");
		assertEquals(o2.toString(), merge.toString());
	}

	public void merge_notEmptyCollectionAndOneEmpty_notEmptyCollection() {
		JSONObject o1 = json("{\"k\":[1]}");
		JSONObject o2 = json("{\"k\":[]}");
		JSONObject merge = JsonUtils.mergeKey(asList(o1, o2), "k");
		assertEquals(o1.toString(), merge.toString());
	}

	private JSONObject json(String s) {
		try {
			return new JSONObject(s);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public void sumKeyInNestedJson_jsonDoesNotContainKey_0() {
		long sum = JsonUtils.sumKeyInNestedJson(json("{}"), "sumKey", "objectKey");
		assertEquals(sum, 0);
	}

	public void sumKeyInNestedJson_singleObject_valueOfKeyToSumInNestedJson() {
		JSONObject json = json("{objectKey : {keyToSum : 3}}");
		long sum = JsonUtils.sumKeyInNestedJson(json, "keyToSum", "objectKey");
		assertEquals(sum, 3);
	}

	public void sumKeyInNestedJson_nestedListContainsKeyToSum_sumOfAllTheKeysInList() {
		JSONObject json = json("{objectKey : [{keyToSum : 1}, {keyToSum : 4}]}");
		long sum = JsonUtils.sumKeyInNestedJson(json, "keyToSum", "objectKey");
		assertEquals(sum, 5);
	}

	public void sumKeyInNestedJson_nestedJsonDoesNotContainKeyToSum_0() {
		JSONObject json = json("{objectKey : {X : 3}}");
		long sum = JsonUtils.sumKeyInNestedJson(json, "keyToSum", "objectKey");
		assertEquals(sum, 0);
	}

	public void sumKeyInNestedJson_nestedJsonListDoesNotContainKey_sumOfAllTheFoundKeysInList() {
		JSONObject json = json("{objectKey : [{keyToSum : 1}, {X : 4}]}");
		long sum = JsonUtils.sumKeyInNestedJson(json, "keyToSum", "objectKey");
		assertEquals(sum, 1);
	}
}
