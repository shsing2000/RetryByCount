/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.samhsing.nifi.processors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

public class RetryByCountProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(RetryByCountProcessor.class);
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void propertyDescriptorsShouldContainExceptedProperties() {
        RetryByCountProcessor processor = new RetryByCountProcessor();
        processor.init(null);
        List<PropertyDescriptor> descriptors = processor.getSupportedPropertyDescriptors();
        Assert.assertEquals("size should be equal", 2, descriptors.size());
        Assert.assertTrue(descriptors.contains(RetryByCountProcessor.COUNTER_ATTR_NAME));
        Assert.assertTrue(descriptors.contains(RetryByCountProcessor.COUNTER_MAX_LIMIT));
    }

    @Test
    public void defaultValuesShouldBeReasonable() {
        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(RetryByCountProcessor.RETRY, 1);
        final List<MockFlowFile> flowfiles = testRunner.getFlowFilesForRelationship(RetryByCountProcessor.RETRY);
        MockFlowFile flowfile = flowfiles.get(0);
        flowfile.assertAttributeEquals("retry.counter", "1");
    }

    @Test
    public void FlowFileWithAttirbute() {
        testRunner.setProperty(RetryByCountProcessor.COUNTER_ATTR_NAME, "retry.test.counter");
        testRunner.setProperty(RetryByCountProcessor.COUNTER_MAX_LIMIT, "5");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("retry.test.counter", "3");
        testRunner.enqueue(new byte[0], attrs);
        testRunner.run(1);

        testRunner.assertAllFlowFilesTransferred(RetryByCountProcessor.RETRY, 1);
    }

    public void AttributeIsOverTheLimit() {

    }

    public void AttributeIsUnderTheLimit() {

    }

}
