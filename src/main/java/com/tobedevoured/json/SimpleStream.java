package com.tobedevoured.json;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Simple Stream handler for JSON
 */
public class SimpleStream {

    private static final Logger logger = LoggerFactory.getLogger(SimpleStream.class);
    public static final int MAX_BUFFER_MALFORMED_FRAGMENTS = 1;
    public static final int DEFAULT_BUFFER_SIZE = 8092;
    public static final List EMPTY_LIST = new ArrayList();

    StringBuilder buffer;
    Function<Object, Object> callback;
    int bufferSize;

    int malformedFragmentAttempts = 1;

    /**
     * Create instance with the default buffer size of 8092
     */
    public SimpleStream() {
        this(DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create instance with specific buffer size
     *
     * @param bufferSize int buffer size
     */
    public SimpleStream(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = new StringBuilder(bufferSize);
    }

    /**
     * Resets the stream handler
     */
    public void reset() {
        buffer = new StringBuilder(bufferSize);
        malformedFragmentAttempts = 0;
    }

    /**
     * Flush the buffer and return all valid entities
     *
     * @return List of json entities
     * @throws StreamException parser error
     */
    public List flush() throws StreamException {
       return processBuffer(0);
    }

    /**
     * Callback to be executed when an entity is parsed
     *
     * @param callback Function
     */
    public void setCallback(Function<Object, Object> callback) {
        this.callback = callback;
    }

    /**
     * Stream JSON from url
     *
     * @param url String
     * @param timeout int
     * @throws StreamException parser error
     */
    public void streamFromUrl(final String url, final int timeout) throws StreamException {
        logger.info("Streaming JSON from {}", url);

        CloseableHttpClient httpclient = HttpClients
            .custom()
            .setKeepAliveStrategy((response, context) -> timeout * 1000)
            .build();

        HttpGet httpget = new HttpGet(url);

        try {
            CloseableHttpResponse response = httpclient.execute(httpget);

            try {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    BufferedReader reader =
                            new BufferedReader(new InputStreamReader(entity.getContent()));
                    try {
                        String line;
                        while ((line = reader.readLine( )) != null) {
                            logger.debug("Streaming line: {}", line);
                            stream(line);
                        }
                    } finally {
                        flush();
                        reader.close();
                    }
                }
            } finally {
                response.close();
            }

        } catch (Exception e) {
            logger.error("Unexpected error", e);
            throw new StreamException(e);
        }
    }

    /**
     * Stream in json. When the buffer size is reached, the buffer is processed into json entities.
     *
     * @param stream String
     * @return List
     * @throws StreamException parser error
     */
    public List stream(final String stream) throws StreamException {
        if (buffer.length() > 0) {
            logger.info("Merging fragments {}||{}", buffer, stream);
        }

        buffer.append(stream);

        if (buffer.length() >= bufferSize) {
            return processBuffer(MAX_BUFFER_MALFORMED_FRAGMENTS);
        }

        return EMPTY_LIST;
    }

    /**
     * Process the buffer into json entities. When allow for multiple attempts for malformed json, which
     * can be caused by a json fragment in the stream.
     *
     * @param allowedMalformedAttempts int
     * @return List
     * @throws StreamException parser error
     */
    public List processBuffer(int allowedMalformedAttempts) throws StreamException {
        JSONParser parser = new JSONParser();
        JsonStreamHandler streamHandler = new JsonStreamHandler();

        List entities = new ArrayList();

        int pos = 0;

        // Check if the buffer is crammed to the brim
        boolean bufferOverflow = buffer.length() > bufferSize;

        while( pos < buffer.length() -1 ) {
            String fragment = buffer.substring(pos);

            try {
                parser.parse(fragment, streamHandler);
            } catch (ParseException e) {

                if (ParseException.ERROR_UNEXPECTED_CHAR == e.getErrorType()) {
                    // Check if should wait for more streaming json before declaring it malformed
                    if (malformedFragmentAttempts < allowedMalformedAttempts) {
                        malformedFragmentAttempts++;
                    } else {
                        throw new StreamException(e);
                    }

                } else if (ParseException.ERROR_UNEXPECTED_TOKEN != e.getErrorType()) {
                    throw new StreamException(e);
                }

                logger.debug("detected json fragment, buffering for the rest: {}", fragment);
                return entities;
            }

            // increment the position in the buffer
            int currentPos = pos;
            pos = pos + parser.getPosition() + 1;

            // Create entity from buffer
            Object val = null;
            try {
                val = parser.parse(buffer.substring(currentPos, pos).trim());
                buffer.delete(currentPos, pos);
                pos = 0;
            } catch (ParseException e) {
                throw new StreamException(e);
            }

            // valid entity created, reset the malformed counter
            malformedFragmentAttempts = 0;

            if (callback != null) {
                logger.info("Executing callback with {}", val);
                try {
                    callback.apply(val);
                } catch(Exception e) {
                    logger.error("Failed to execute callback", e);
                    throw new StreamException(e);
                }
            }

            entities.add(val);
        }

        // Compact the buffer after an overflow
        if (buffer.length() < bufferSize && bufferOverflow) {
            buffer.setLength(bufferSize);
        }

        return entities;
    }
}
