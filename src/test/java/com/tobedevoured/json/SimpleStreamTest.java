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

import com.google.common.collect.ImmutableMap;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.Assert.*;

/**
 * Created by mguymon on 3/8/16.
 */
public class SimpleStreamTest {

    SimpleStream simpleStream;


    @Before
    public void setup() {
        simpleStream = new SimpleStream();
    }

    @Test
    public void testFragmentedStream() throws ParseException {
        List entities = simpleStream.stream("{\"test\": \"this is ");
        entities.addAll(simpleStream.stream("a test\"} [1,2,3]"));

        Map<String, String> test = ImmutableMap.of("test", "this is a test");
        assertEquals(entities.get(0), test);

        assertEquals(entities.get(1), Arrays.asList(1L,2L,3L));
    }

    @Test
    public void testEmptyStream() throws ParseException {
        List entities = simpleStream.stream("");

        assertEquals(new ArrayList<>(0), entities);
    }

    @Test
    public void testCallback() throws ParseException {
        final AtomicBoolean isCalledBack = new AtomicBoolean(false);
        simpleStream.setCallback(new Function<Object, Object>() {

            @Override
            public Object apply(Object entity) {
                assertNotNull(entity);
                isCalledBack.set(true);
                return null;
            }
        });

        List entities = simpleStream.stream("{\"test\": \"this is a test\"} [1,2,3]");

        assertTrue("callback was called", isCalledBack.get());

        Map<String, String> expectedEntity  = ImmutableMap.of("test", "this is a test");
        assertEquals(expectedEntity, entities.get(0));

        assertEquals(Arrays.asList(1L,2L,3L), entities.get(1));
    }
}
