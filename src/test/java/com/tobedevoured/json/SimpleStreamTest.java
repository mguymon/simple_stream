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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Created by mguymon on 3/8/16.
 */
public class SimpleStreamTest {

    SimpleStream simpleStream;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);


    @Before
    public void setup() {
        simpleStream = new SimpleStream();
    }


    @Test
    public void testStreamFromUrl() throws StreamException {
        stubFor(get(urlEqualTo("/test"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/json")
                        .withBody("{ \"test\": \n  true }")));

        final Map results = new HashMap();
        simpleStream.setCallback(new Function<Object, Object>() {

            @Override
            public Object apply(Object entity) {
                assertNotNull(entity);

                results.putAll((Map)entity);

                return null;
            }
        });

        simpleStream.streamFromUrl("http://localhost:8089/test", 30);

        Map<String, Boolean> test = ImmutableMap.of("test", true);
        assertEquals(results, test);
    }

    @Test
    public void testFragmentedString() throws StreamException {
        List entities = simpleStream.stream("{\"test\": \"this is ");
        entities.addAll(simpleStream.stream("a test\"} [1,2,3]"));

        Map<String, String> test = ImmutableMap.of("test", "this is a test");
        assertEquals(test, entities.get(0));

        assertEquals( Arrays.asList(1L,2L,3L), entities.get(1));
    }

    @Test
    public void testFragmentedArray() throws StreamException {
        simpleStream.stream("{\"cursor\": \"123\", \"check\": [1,2");
        List entities = simpleStream.stream(",3]}");

        JSONObject map = ((JSONObject)entities.get(0));
        assertEquals("123", map.get("cursor"));
        assertThat(map.get("check"), (Matcher)hasItems(1L, 2L, 3L));
    }

    @Test
    public void testFragmentedBoolean() throws StreamException {
        simpleStream.stream("{\"cursor\": \"123\", \"starred\": fals");
        List entities = simpleStream.stream("e}");

        Map<String, Object> expectedEntity  = ImmutableMap.of("cursor", "123", "starred", false);
        assertEquals(expectedEntity, entities.get(0));
    }

    @Test
    public void testFragmentedInteger() throws StreamException {
        simpleStream.stream("{\"cursor\": \"123\", \"amount\": 123");
        List entities = simpleStream.stream("4}");

        Map<String, Object> expectedEntity  = ImmutableMap.of("cursor", "123", "amount", 1234L);
        assertEquals(expectedEntity, entities.get(0));
    }

    @Test
    public void testEmptyStream() throws StreamException {
        List entities = simpleStream.stream("");

        assertEquals(new ArrayList<>(0), entities);
    }

    @Test
    public void testCallback() throws StreamException {
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
