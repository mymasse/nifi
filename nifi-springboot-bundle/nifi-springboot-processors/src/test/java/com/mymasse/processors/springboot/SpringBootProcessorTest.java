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
package com.mymasse.processors.springboot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class SpringBootProcessorTest {

    private static final String TEST_APPLICATION_CLASS = "com.mymasse.nifi.NifiBootApplication";
    private static final String TEST_BOOT_JAR = "src/test/resources/bidirectional.jar";

    @Test
    public void validationTests() {
        TestRunner runner = TestRunners.newTestRunner(SpringBootProcessor.class);
        runner.assertNotValid();

        runner = TestRunners.newTestRunner(SpringBootProcessor.class);
        runner.setProperty(SpringBootProcessor.BOOT_CLASS, TEST_APPLICATION_CLASS);
        runner.assertNotValid();

        runner = TestRunners.newTestRunner(SpringBootProcessor.class);
        runner.setProperty(SpringBootProcessor.BOOT_JAR_PATH, "not/found/file.jar");
        runner.assertNotValid();

        runner = TestRunners.newTestRunner(SpringBootProcessor.class);
        runner.setProperty(SpringBootProcessor.BOOT_JAR_PATH, TEST_BOOT_JAR);
        runner.assertNotValid();

        runner = TestRunners.newTestRunner(SpringBootProcessor.class);
        runner.setProperty(SpringBootProcessor.BOOT_CLASS, TEST_APPLICATION_CLASS);
        runner.setProperty(SpringBootProcessor.BOOT_JAR_PATH, TEST_BOOT_JAR);
        runner.assertValid();
    }

    @Test
    public void runTest() {
        TestRunner runner = TestRunners.newTestRunner(SpringBootProcessor.class);
        runner.setProperty(SpringBootProcessor.BOOT_CLASS, TEST_APPLICATION_CLASS);
        runner.setProperty(SpringBootProcessor.BOOT_JAR_PATH, TEST_BOOT_JAR);
        runner.assertValid();

        runner.enqueue("Hello".getBytes());

        runner.run(1, false);

        List<MockFlowFile> ffList = runner.getFlowFilesForRelationship(SpringBootProcessor.REL_SUCCESS);
        assertTrue(ffList.size() == 1);
        assertEquals("Yes it works", ffList.get(0).getAttribute("NiFiContext"));
        runner.shutdown();
    }

}
