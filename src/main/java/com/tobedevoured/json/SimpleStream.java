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

import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import org.slf4j.impl.SimpleLoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SimpleStream {


    private static final List EMPTY_LIST = new ArrayList();
    private static final Logger logger = LoggerFactory.getLogger(SimpleStream.class);


    class JsonStreamHandler implements ContentHandler {

        int openEntity = 0;

        @Override
        public void startJSON() throws ParseException, IOException {

            logger.debug("startJson");
        }

        @Override
        public void endJSON() throws ParseException, IOException {
            logger.debug("endJson");
        }

        @Override
        public boolean startObject() throws ParseException, IOException {
            openEntity++;
            logger.debug("startObject of {}", openEntity);
            return true;
        }

        @Override
        public boolean endObject() throws ParseException, IOException {
            logger.debug("endObject of {}", openEntity);
            openEntity--;
            return openEntity > 0;
        }

        @Override
        public boolean startObjectEntry(String key) throws ParseException, IOException {
            return true;
        }

        @Override
        public boolean endObjectEntry() throws ParseException, IOException {
            return true;
        }

        @Override
        public boolean startArray() throws ParseException, IOException {
            openEntity++;
            logger.debug("startArray of {}", openEntity);
            return true;
        }

        @Override
        public boolean endArray() throws ParseException, IOException {
            logger.debug("endArray of {}", openEntity);
            openEntity--;
            return openEntity > 0;
        }

        @Override
        public boolean primitive(Object value) throws ParseException, IOException {
            return true;
        }
    }

    StringBuilder buffer = new StringBuilder();
    Function<Object, Object> callback;

    public void reset() {
        buffer = new StringBuilder();
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
        JSONParser parser = new JSONParser();
        JsonStreamHandler streamHandler = new JsonStreamHandler();

        if (buffer.length() > 0) {
            logger.info("Merging fragments {}||{}", buffer, stream);
        }

        buffer.append(stream);

        List entities = new ArrayList();

        int pos = 0;
        while( pos < buffer.length() -1 ) {
            String fragment = buffer.substring(pos);

            try {
                parser.parse(fragment, streamHandler);
            } catch (ParseException e) {
                if (ParseException.ERROR_UNEXPECTED_TOKEN != e.getErrorType()) {
                    throw new StreamException(e);
                }

                logger.debug("detected json fragment, buffering for the rest: {}", fragment);
                return entities;
            }

            int currentPos = pos;
            pos = pos + parser.getPosition() + 1;

            Object val = null;
            try {
                val = parser.parse(buffer.substring(currentPos, pos).trim());
                buffer.delete(currentPos, pos);
                pos = 0;
            } catch (ParseException e) {
                throw new StreamException(e);
            }

            if (callback != null) {
                callback.apply(val);
            }

            entities.add(val);
        }

        return entities;
    }

    public static void main(String args[]) throws StreamException {
        SimpleStream stream = new SimpleStream();
        stream.stream("{\"test\": \"this is ");
        stream.stream("a test\"} [1,2,3]");
    }
}
