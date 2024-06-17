/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.workflow.dolphinscheduler;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.CollectionType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;
import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static org.apache.flink.table.gateway.workflow.dolphinscheduler.http.Constants.YYYY_MM_DD_HH_MM_SS;

public final class JSONUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JSONUtils.class);

    static {
        LOG.info("init timezone: {}", TimeZone.getDefault());
    }

    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
                    .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
                    .configure(REQUIRE_SETTERS_FOR_GETTERS, true)
                    .configure(FAIL_ON_EMPTY_BEANS, false)
                    .addModule(
                            new SimpleModule()
                                    .addSerializer(
                                            LocalDateTime.class, new LocalDateTimeSerializer())
                                    .addDeserializer(
                                            LocalDateTime.class, new LocalDateTimeDeserializer()))
                    .defaultTimeZone(TimeZone.getDefault())
                    .defaultDateFormat(new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS))
                    .build();

    private JSONUtils() {
        throw new UnsupportedOperationException("Construct JSONUtils");
    }

    public static synchronized void setTimeZone(TimeZone timeZone) {
        objectMapper.setTimeZone(timeZone);
    }

    public static ArrayNode createArrayNode() {
        return objectMapper.createArrayNode();
    }

    public static ObjectNode createObjectNode() {
        return objectMapper.createObjectNode();
    }

    public static JsonNode toJsonNode(Object obj) {
        return objectMapper.valueToTree(obj);
    }

    /**
     * json representation of object
     *
     * @param object object
     * @param feature feature
     * @return object to json string
     */
    public static String toJsonString(Object object, SerializationFeature feature) {
        try {
            ObjectWriter writer = objectMapper.writer(feature);
            return writer.writeValueAsString(object);
        } catch (IOException e) {
            LOG.error("object to json exception!", e);
            throw new UncheckedIOException("object to json exception!", e);
        }
    }

    /**
     * This method deserializes the specified Json into an object of the specified class. It is not
     * suitable to use if the specified class is a generic type since it will not have the generic
     * type information because of the Type Erasure feature of Java. Therefore, this method should
     * not be used if the desired type is a generic type. Note that this method works fine if the
     * any of the fields of the specified object are generics, just the object itself should not be
     * a generic type.
     *
     * @param json the string from which the object is to be deserialized
     * @param clazz the class of T
     * @param <T> T
     * @return an object of type T from the string classOfT
     */
    public static @Nullable <T> T parseObject(String json, Class<T> clazz) {
        if (Strings.isNullOrEmpty(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            LOG.error("Parse object exception, jsonStr: {}, class: {}", json, clazz, e);
            throw new UncheckedIOException(
                    String.format("Parse object exception, jsonStr: %s, class: %s", json, clazz),
                    e);
        }
    }

    /**
     * deserialize
     *
     * @param src byte array
     * @param clazz class
     * @param <T> deserialize type
     * @return deserialize type
     */
    public static <T> T parseObject(byte[] src, Class<T> clazz) {
        if (src == null) {
            return null;
        }
        String json = new String(src, UTF_8);
        return parseObject(json, clazz);
    }

    /**
     * json to list
     *
     * @param json json string
     * @param clazz class
     * @param <T> T
     * @return list
     */
    public static <T> List<T> toList(String json, Class<T> clazz) {
        if (Strings.isNullOrEmpty(json)) {
            return Collections.emptyList();
        }

        try {
            CollectionType listType =
                    objectMapper.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
            return objectMapper.readValue(json, listType);
        } catch (IOException e) {
            LOG.error("parse list exception!", e);
            throw new UncheckedIOException("parse list exception!", e);
        }
    }

    /**
     * check json object valid
     *
     * @param json json
     * @return true if valid
     */
    public static boolean checkJsonValid(String json) {

        if (Strings.isNullOrEmpty(json)) {
            return false;
        }

        try {
            objectMapper.readTree(json);
            return true;
        } catch (IOException e) {
            LOG.error("check json object valid exception!", e);
            throw new UncheckedIOException("check json object valid exception!", e);
        }
    }

    /**
     * Method for finding a JSON Object field with specified name in this node or its child nodes,
     * and returning value it has. If no matching field is found in this node or its descendants,
     * returns null.
     *
     * @param jsonNode json node
     * @param fieldName Name of field to look for
     * @return Value of first matching node found, if any; null if none
     */
    public static String findValue(JsonNode jsonNode, String fieldName) {
        JsonNode node = jsonNode.findValue(fieldName);

        if (node == null) {
            return null;
        }

        return node.asText();
    }

    /**
     * json to map {@link #toMap(String, Class, Class)}
     *
     * @param json json
     * @return json to map
     */
    public static Map<String, String> toMap(String json) {
        return parseObject(json, new TypeReference<Map<String, String>>() {});
    }

    /**
     * json to map
     *
     * @param json json
     * @param classK classK
     * @param classV classV
     * @param <K> K
     * @param <V> V
     * @return to map
     */
    public static <K, V> Map<K, V> toMap(String json, Class<K> classK, Class<V> classV) {
        if (Strings.isNullOrEmpty(json)) {
            return Collections.emptyMap();
        }

        try {
            return objectMapper.readValue(json, new TypeReference<Map<K, V>>() {});
        } catch (IOException e) {
            LOG.error("json to map exception!", e);
            throw new UncheckedIOException("json to map exception!", e);
        }
    }

    /**
     * from the key-value generated json to get the str value no matter the real type of value
     *
     * @param json the json str
     * @param nodeName key
     * @return the str value of key
     */
    public static String getNodeString(String json, String nodeName) {
        try {
            JsonNode rootNode = objectMapper.readTree(json);
            JsonNode jsonNode = rootNode.findValue(nodeName);
            if (Objects.isNull(jsonNode)) {
                return "";
            }
            return jsonNode.isTextual() ? jsonNode.asText() : jsonNode.toString();
        } catch (JsonProcessingException e) {
            return "";
        }
    }

    /**
     * json to object
     *
     * @param json json string
     * @param type type reference
     * @param <T>
     * @return return parse object
     */
    public static <T> T parseObject(String json, TypeReference<T> type) {
        if (Strings.isNullOrEmpty(json)) {
            return null;
        }

        try {
            return objectMapper.readValue(json, type);
        } catch (IOException e) {
            LOG.error("json to map exception!", e);
            throw new UncheckedIOException("json to map exception!", e);
        }
    }

    /**
     * object to json string
     *
     * @param object object
     * @return json string
     */
    public static String toJsonString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new UncheckedIOException("Object json deserialization exception.", e);
        }
    }

    public static String toPrettyJsonString(Object object) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (IOException e) {
            throw new UncheckedIOException("Object json deserialization exception.", e);
        }
    }

    /**
     * serialize to json byte
     *
     * @param obj object
     * @param <T> object type
     * @return byte array
     */
    public static <T> byte[] toJsonByteArray(T obj) {
        if (obj == null) {
            return null;
        }
        String json = "";
        try {
            json = toJsonString(obj);
        } catch (Exception e) {
            LOG.error("json serialize exception.", e);
        }

        return json.getBytes(UTF_8);
    }

    public static ObjectNode parseObject(String text) {
        try {
            if (StringUtils.isEmpty(text)) {
                return parseObject(text, ObjectNode.class);
            } else {
                return (ObjectNode) objectMapper.readTree(text);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("String json deserialization exception.", e);
        }
    }

    public static ArrayNode parseArray(String text) {
        try {
            return (ArrayNode) objectMapper.readTree(text);
        } catch (IOException e) {
            throw new UncheckedIOException("Json deserialization exception.", e);
        }
    }

    public static <T> T convertToEntity(LinkedHashMap<String, String> map, Class<T> clazz) {
        return objectMapper.convertValue(map, clazz);
    }

    public static <T> List<T> convertToEntityList(List<Object> list, Class<T> clazz) {
        CollectionType listType =
                objectMapper.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
        return objectMapper.convertValue(list, listType);
    }
    /** json serializer */
    public static class JsonDataSerializer extends JsonSerializer<String> {

        @Override
        public void serialize(String value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeRawValue(value);
        }
    }

    /** json data deserializer */
    public static class JsonDataDeserializer extends JsonDeserializer<String> {

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            if (node instanceof TextNode) {
                return node.asText();
            } else {
                return node.toString();
            }
        }
    }

    public static class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS);

        @Override
        public void serialize(
                LocalDateTime value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value.format(formatter));
        }
    }

    public static class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS);

        @Override
        public LocalDateTime deserialize(JsonParser p, DeserializationContext context)
                throws IOException {
            return LocalDateTime.parse(p.getValueAsString(), formatter);
        }
    }
}
