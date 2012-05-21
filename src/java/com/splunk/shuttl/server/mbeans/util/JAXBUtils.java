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
package com.splunk.shuttl.server.mbeans.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 * @author kpakkirisamy
 *
 */
public class JAXBUtils {
    public static void save(Class confClass, Object confObj, String xmlFilePath)
	    throws Exception {
	JAXBContext context = JAXBContext.newInstance(confClass);
	Marshaller m = context.createMarshaller();
	m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
	m.marshal(confObj, new FileWriter(xmlFilePath));
	
    }

    public static Object refresh(Class confClass,
 String xmlFilePath)
	    throws JAXBException, FileNotFoundException {
	JAXBContext context = JAXBContext.newInstance(confClass);
	    Unmarshaller um = context.createUnmarshaller();
	return um.unmarshal(new FileReader(xmlFilePath));
    }

}
