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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mymasse.processors.springboot.SpringDataExchanger.SpringResponse;

@TriggerWhenEmpty
@Tags({ "Spring", "Boot", "Message", "Get", "Put", "Integration" })
@CapabilityDescription("A Processor that supports sending and receiving data from application defined in "
        + "Spring Boot Application via predefined in/out MessageChannels.")
public class SpringBootProcessor extends AbstractProcessor {

    private final Logger logger = LoggerFactory.getLogger(SpringBootProcessor.class);

    public static final PropertyDescriptor BOOT_CLASS = new PropertyDescriptor.Builder()
            .name("Spring Boot Application class name")
            .description("The Spring Boot Application class name to run")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BOOT_JAR_PATH = new PropertyDescriptor.Builder()
            .name("Spring Boot Application Jar")
            .description("Path to the Spring Boot Application jar.")
            .addValidator(StandardValidators.createURLorFileValidator())
            .required(true)
            .build();
    public static final PropertyDescriptor SEND_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Send Timeout")
            .description("Timeout for sending data to Spring Boot Application. Defaults to 0.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor RECEIVE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Receive Timeout")
            .description("Timeout for receiving date from Spring Boot Application. Defaults to 0.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    // ====

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description(
                    "All FlowFiles that are successfully received from Spring Boot Application are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description(
                    "All FlowFiles that cannot be sent to Spring Boot Application are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    // =======

    private volatile String springBootClass;

    private volatile String springBootJarFile;

    private volatile long sendTimeout;

    private volatile long receiveTimeout;

    private volatile SpringDataExchanger exchanger;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(BOOT_CLASS);
        _propertyDescriptors.add(BOOT_JAR_PATH);
        _propertyDescriptors.add(SEND_TIMEOUT);
        _propertyDescriptors.add(RECEIVE_TIMEOUT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        SpringBootConfigValidator validator = new SpringBootConfigValidator();
        return Collections.singletonList(validator.validate(BOOT_JAR_PATH.getName(), null, validationContext));
    }

    @OnScheduled
    public void onScheduled(final ProcessContext processContext) {
        this.springBootClass = processContext.getProperty(BOOT_CLASS).getValue();
        this.springBootJarFile = processContext.getProperty(BOOT_JAR_PATH).getValue();

        String stStr = processContext.getProperty(SEND_TIMEOUT).getValue();
        this.sendTimeout = stStr == null ? 0 : FormatUtils.getTimeDuration(stStr, TimeUnit.MILLISECONDS);

        String rtStr = processContext.getProperty(RECEIVE_TIMEOUT).getValue();
        this.receiveTimeout = rtStr == null ? 0 : FormatUtils.getTimeDuration(rtStr, TimeUnit.MILLISECONDS);

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Initializing Spring Boot Application from " + this.springBootJarFile);
            }
            this.exchanger = SpringBootFactory.createSpringBootDelegate(this.springBootClass, this.springBootJarFile);
        } catch (Exception e) {
            throw new IllegalStateException("Failed while initializing Spring Boot Application", e);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Successfully initialized Spring Boot Application from " + this.springBootJarFile);
        }
    }

    @OnStopped
    public void onStopped(ProcessContext processContext) {
        if (this.exchanger != null) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Closing Spring Boot Application from " + this.springBootJarFile);
                }
                this.exchanger.close();
                if (logger.isInfoEnabled()) {
                    logger.info("Successfully closed Spring Boot Application from " + this.springBootJarFile);
                }
            } catch (Exception e) {
                getLogger().warn("Failed while closing Spring Boot Application Context", e);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            this.sendToSpring(flowFile, processContext, processSession);
        }
        this.receiveFromSpring(processSession);
    }

    private void sendToSpring(FlowFile flowFileToProcess, ProcessContext context, ProcessSession processSession) {
        byte[] payload = this.extractMessage(flowFileToProcess, processSession);
        boolean sent = false;

        try {
            sent = this.exchanger.send(payload, flowFileToProcess.getAttributes(), this.sendTimeout);
            if (sent) {
                processSession.getProvenanceReporter().send(flowFileToProcess, this.springBootJarFile);
                processSession.remove(flowFileToProcess);
            } else {
                processSession.transfer(processSession.penalize(flowFileToProcess), REL_FAILURE);
                this.getLogger().error("Timed out while sending FlowFile to Spring Boot Application " + this.springBootJarFile);
                context.yield();
            }
        } catch (Exception e) {
            processSession.transfer(flowFileToProcess, REL_FAILURE);
            this.getLogger().error("Failed while sending FlowFile to Spring Boot Application " + this.springBootJarFile + "; " + e.getMessage(), e);
            context.yield();
        }
    }

    private byte[] extractMessage(FlowFile flowFile, ProcessSession processSession) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });
        return messageContent;
    }

    private void receiveFromSpring(ProcessSession processSession) {
        final SpringResponse<?> msgFromSpring = this.exchanger.receive(this.receiveTimeout);
        if (msgFromSpring != null) {
            FlowFile flowFileToProcess = processSession.create();
            flowFileToProcess = processSession.write(flowFileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    Object payload = msgFromSpring.getPayload();
                    byte[] payloadBytes = payload instanceof String ? ((String) payload).getBytes() : (byte[]) payload;
                    out.write(payloadBytes);
                }
            });
            flowFileToProcess = processSession.putAllAttributes(flowFileToProcess, this.extractFlowFileAttributesFromMessageHeaders(msgFromSpring.getHeaders()));
            processSession.transfer(flowFileToProcess, REL_SUCCESS);
            processSession.getProvenanceReporter().receive(flowFileToProcess, this.springBootJarFile);
        }
    }

    private Map<String, String> extractFlowFileAttributesFromMessageHeaders(Map<String, Object> messageHeaders) {
        Map<String, String> attributes = new HashMap<>();
        for (Entry<String, Object> entry : messageHeaders.entrySet()) {
            if (entry.getValue() instanceof String) {
                attributes.put(entry.getKey(), (String) entry.getValue());
            }
        }
        return attributes;
    }

    static class SpringBootConfigValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String bootClass = context.getProperty(BOOT_CLASS).getValue();
            String bootJarPath = context.getProperty(BOOT_JAR_PATH).getValue();

            StringBuilder invalidationMessageBuilder = new StringBuilder();
            if (bootClass != null && bootJarPath != null) {
                validateJar(bootJarPath, invalidationMessageBuilder);

                if (invalidationMessageBuilder.length() == 0 && !isClassResolvable(bootClass, new File(bootJarPath))) {
                    invalidationMessageBuilder.append("'Spring Boot Application class name' can not be located in the provided jar.");
                }
            } else if (StringUtils.isEmpty(bootClass)) {
                invalidationMessageBuilder.append("'Spring Boot Application class name' must not be empty.");
            } else {
                if (StringUtils.isEmpty(bootJarPath)) {
                    invalidationMessageBuilder.append("'Spring Boot Application Jar' must not be empty.");
                } else {
                    validateJar(bootJarPath, invalidationMessageBuilder);
                }
            }

            String invalidationMessage = invalidationMessageBuilder.toString();
            ValidationResult vResult = invalidationMessage.length() == 0
                    ? new ValidationResult.Builder().subject(subject).input(input)
                            .explanation("Spring configuration '" + bootClass + "' is resolvable against provided jar '" + bootJarPath + "'.")
                            .valid(true).build()
                    : new ValidationResult.Builder().subject(subject).input(input)
                            .explanation("Spring configuration '" + bootClass + "' is NOT resolvable against provided jar '" + bootJarPath + "'. Validation message: " + invalidationMessage)
                            .valid(false).build();

            return vResult;
        }

        private static void validateJar(String bootJarPath, StringBuilder invalidationMessageBuilder) {
            File bootJarPathFile = new File(bootJarPath);
            if (!bootJarPathFile.exists()) {
                invalidationMessageBuilder.append("'Spring Boot Application Jar' does not exist. Was '" + bootJarPathFile.getAbsolutePath() + "'.");
            } else if (!bootJarPathFile.isFile()) {
                invalidationMessageBuilder.append("'Spring Boot Application Jar' must point to a file. Was '" + bootJarPathFile.getAbsolutePath() + "'.");
            }
        }

        private static boolean isClassResolvable(String classFilename, File bootJarPathFile) {
            List<URL> urls = new ArrayList<>();
            URLClassLoader parentLoader = (URLClassLoader) SpringBootProcessor.class.getClassLoader();
            urls.addAll(Arrays.asList(parentLoader.getURLs()));
            urls.addAll(SpringBootFactory.gatherClassPathUrls(bootJarPathFile.getAbsolutePath()));
            boolean resolvable = false;
            try (URLClassLoader throwawayCl = new URLClassLoader(urls.toArray(new URL[] {}), null)) {
                resolvable = throwawayCl.loadClass(classFilename) != null;
            } catch (IOException e) {
                // ignore since it can only happen on CL.close()
            } catch (ClassNotFoundException e) {
                resolvable = false;
            }
            return resolvable;
        }
    }

}
