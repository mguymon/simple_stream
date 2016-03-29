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

public class SimpleStream {

    private static final Logger logger = LoggerFactory.getLogger(SimpleStream.class);
    public static final int MAX_BUFFER_MALFORMED_FRAGMENTS = 1;
    public static final int DEFAULT_BUFFER_SIZE = 8092;
    public static final List EMPTY_LIST = new ArrayList();

    StringBuilder buffer;
    Function<Object, Object> callback;
    int bufferSize;

    int malformedFragmentAttempts = 1;

    public SimpleStream() {
        this(DEFAULT_BUFFER_SIZE);
    }


    public SimpleStream(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = new StringBuilder(bufferSize);
    }

    public void reset() {
        buffer = new StringBuilder();
        malformedFragmentAttempts = 0;
    }

    public List flush() throws StreamException {
       return processBuffer(0);
    }

    public void setCallback(Function<Object, Object> callback) {
        this.callback = callback;
    }

    public void streamFromUrl(final String url, final Integer timeout) throws StreamException {
        logger.info("Streaming JSON from {}", url);

        CloseableHttpClient httpclient = HttpClients
                .custom()
                .setKeepAliveStrategy(
                    new ConnectionKeepAliveStrategy() {
                        @Override
                        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                            return timeout * 1000;
                        }
                    }
                )
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
                        String line = null;
                        while ((line = reader.readLine( )) != null) {
                            logger.debug("Streaming line: {}", line);
                            stream(line);
                        }
                    } finally {
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

    public List processBuffer(int allowedMalformedAttempts) throws StreamException {
        JSONParser parser = new JSONParser();
        JsonStreamHandler streamHandler = new JsonStreamHandler();

        List entities = new ArrayList();

        int pos = 0;

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

        return entities;
    }
}
