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

import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SimpleStream {

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
            openEntity--;
            logger.debug("endObject of {}", openEntity);
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
            openEntity--;
            logger.debug("endArray of {}", openEntity);
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

    public List stream(String stream) throws ParseException {
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
            } catch ( ParseException pe ) {
                if (ParseException.ERROR_UNEXPECTED_TOKEN != pe.getErrorType()) {
                    throw pe;
                }

                logger.debug("detected json fragment, buffering for the rest: {}", fragment);
                return entities;
            }

            int currentPos = pos;
            pos = pos + parser.getPosition() + 1;

            String jsonEntity = buffer.substring(currentPos, pos);
            Object val = parser.parse(buffer.substring(currentPos, pos).trim());

            if (callback != null) {
                callback.apply(val);
            }

            entities.add(val);
        }

        reset();

        return entities;
    }

    public static void main(String args[]) throws ParseException {
        SimpleStream stream = new SimpleStream();
        stream.stream("{\"test\": \"this is ");
        stream.stream("a test\"} [1,2,3]");
    }
}
