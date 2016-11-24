package com.mymasse.processors.springboot;

import java.io.Closeable;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;

/**
 * Strategy to support type-safe data exchange between NiFi and Spring. It exposes send/receive operations with arguments that are independent of both frameworks simplifying conversion from
 * {@link Message} to {@link FlowFile} and vice versa.
 */
public interface SpringDataExchanger extends Closeable {

    /**
     * Sends data to Spring
     *
     * @param payload
     *            data that will be used as a payload or {@link Message}
     * @param headers
     *            map that will be used to construct {@link MessageHeaders}
     * @param timeout
     *            value to pass to the {@link MessageChannel#send(Message, long)} operation.
     * @return 'true' if message was sent and 'false'otherwise.
     */
    <T> boolean send(T payload, Map<String, ?> headers, long timeout);

    /**
     * Receives data from Spring
     *
     * @param timeout
     *            value to pass to {@link PollableChannel#receive(long)} operation
     * @return {@link SpringResponse} representing <i>content</i> (key) and <i>attributes</i> of the FlowFile to be constructed.
     */
    <T> SpringResponse<T> receive(long timeout);

    public static class SpringResponse<T> {
        private final T payload;
        private final Map<String, Object> headers;

        public SpringResponse(T payload, Map<String, Object> headers) {
            this.payload = payload;
            this.headers = headers;
        }

        public T getPayload() {
            return payload;
        }

        public Map<String, Object> getHeaders() {
            return headers;
        }
    }
}
