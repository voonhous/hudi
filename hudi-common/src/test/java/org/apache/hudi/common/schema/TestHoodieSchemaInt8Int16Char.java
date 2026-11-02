/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.schema;

import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.internal.utils.Conversions;
import org.apache.hudi.common.schema.mapping.HoodieSchemaEquivalence;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * INT8, INT16 and CHAR(n) (RFC-88 rollout step 2): their Avro JSON form, how readers without the logical types
 * degrade, and how promotion, compatibility, projection and evolution treat them.
 */
public class TestHoodieSchemaInt8Int16Char {

  private static final String INT8_JSON = "{\"type\":\"int\",\"logicalType\":\"int8\"}";
  private static final String INT16_JSON = "{\"type\":\"int\",\"logicalType\":\"int16\"}";
  private static final String CHAR_10_JSON = "{\"type\":\"string\",\"logicalType\":\"char\",\"length\":10}";

  // -------------------------------------------------------------------------
  //  Factories and the Avro JSON form
  // -------------------------------------------------------------------------

  @Test
  void factoriesProduceTheHudiLogicalTypes() {
    HoodieSchema int8 = HoodieSchema.createInt8();
    HoodieSchema int16 = HoodieSchema.createInt16();
    HoodieSchema.Char char10 = HoodieSchema.createChar(10);

    assertEquals(HoodieSchemaType.INT8, int8.getType());
    assertEquals(HoodieSchemaType.INT16, int16.getType());
    assertEquals(HoodieSchemaType.CHAR, char10.getType());
    assertEquals(10, char10.getLength());

    assertEquals(Schema.Type.INT, int8.toAvroSchema().getType());
    assertEquals(Schema.Type.INT, int16.toAvroSchema().getType());
    assertEquals(Schema.Type.STRING, char10.toAvroSchema().getType());
    assertSame(HoodieSchema.IntLogicalType.int8(), int8.toAvroSchema().getLogicalType());
    assertSame(HoodieSchema.IntLogicalType.int16(), int16.toAvroSchema().getLogicalType());
    assertEquals(8, HoodieSchema.IntLogicalType.int8().getBitWidth());
    assertEquals(16, HoodieSchema.IntLogicalType.int16().getBitWidth());

    assertEquals(INT8_JSON, int8.toString());
    assertEquals(INT16_JSON, int16.toString());
    assertEquals(CHAR_10_JSON, char10.toString());
  }

  @Test
  void createByTypeBuildsTheNarrowIntsAndRejectsCharWithoutLength() {
    assertEquals(HoodieSchema.createInt8(), HoodieSchema.create(HoodieSchemaType.INT8));
    assertEquals(HoodieSchema.createInt16(), HoodieSchema.create(HoodieSchemaType.INT16));
    assertEquals(HoodieSchemaType.INT8, HoodieSchema.createNullable(HoodieSchemaType.INT8).getNonNullType().getType());
    assertThrows(IllegalArgumentException.class, () -> HoodieSchema.create(HoodieSchemaType.CHAR));
    assertThrows(IllegalArgumentException.class, () -> HoodieSchema.createChar(0));
    assertThrows(IllegalArgumentException.class, () -> HoodieSchema.createChar(-1));
  }

  @Test
  void typesArePrimitiveAndClassified() {
    for (HoodieSchemaType type : Arrays.asList(HoodieSchemaType.INT8, HoodieSchemaType.INT16, HoodieSchemaType.CHAR)) {
      assertTrue(type.isPrimitive(), type.name());
      assertFalse(type.isComplex(), type.name());
    }
    assertEquals(Schema.Type.INT, HoodieSchemaType.INT8.toAvroType());
    assertEquals(Schema.Type.INT, HoodieSchemaType.INT16.toAvroType());
    assertEquals(Schema.Type.STRING, HoodieSchemaType.CHAR.toAvroType());
    assertTrue(HoodieSchemaType.INT8.isNarrowInt());
    assertTrue(HoodieSchemaType.INT16.isNarrowInt());
    assertFalse(HoodieSchemaType.INT.isNarrowInt());
    assertTrue(HoodieSchemaType.INT.isIntLike() && HoodieSchemaType.INT8.isIntLike() && HoodieSchemaType.INT16.isIntLike());
    assertFalse(HoodieSchemaType.DATE.isIntLike() || HoodieSchemaType.LONG.isIntLike());
    assertTrue(HoodieSchemaType.STRING.isStringLike() && HoodieSchemaType.CHAR.isStringLike());
    assertFalse(HoodieSchemaType.UUID.isStringLike() || HoodieSchemaType.ENUM.isStringLike());
  }

  @Test
  void existingOrdinalsAreUnchanged() {
    // the new types are appended, so anything that keyed on an ordinal (Kryo writes enums by ordinal) still reads
    assertEquals(HoodieSchemaType.NULL.ordinal() + 1, HoodieSchemaType.INT8.ordinal());
    assertEquals(HoodieSchemaType.NULL.ordinal() + 2, HoodieSchemaType.INT16.ordinal());
    assertEquals(HoodieSchemaType.NULL.ordinal() + 3, HoodieSchemaType.CHAR.ordinal());
    assertEquals(HoodieSchemaType.values().length - 1, HoodieSchemaType.CHAR.ordinal());
  }

  @ParameterizedTest
  @ValueSource(strings = {INT8_JSON, INT16_JSON, CHAR_10_JSON})
  void parseSerializeRoundTrip(String json) {
    HoodieSchema parsed = HoodieSchema.parse(json);
    assertEquals(json, parsed.toString());
    assertEquals(parsed, HoodieSchema.parse(parsed.toString()));
    assertEquals(parsed, HoodieSchema.fromAvroSchema(new Schema.Parser().parse(json)));
  }

  @Test
  void parseRecordWithEveryShape() {
    HoodieSchema record = HoodieSchema.createRecord("r", "ns", null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.createInt8()),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.createInt16()), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createChar(3), null, "ab "),
        HoodieSchemaField.of("d", HoodieSchema.createArray(HoodieSchema.createChar(2))),
        HoodieSchemaField.of("e", HoodieSchema.createMap(HoodieSchema.createInt8()))));
    HoodieSchema parsed = HoodieSchema.parse(record.toString());
    assertEquals(record, parsed);
    assertEquals(HoodieSchemaType.INT8, parsed.getField("a").get().schema().getType());
    assertEquals(HoodieSchemaType.INT16, parsed.getField("b").get().getNonNullSchema().getType());
    assertEquals(3, ((HoodieSchema.Char) parsed.getField("c").get().schema()).getLength());
    assertEquals("ab ", HoodieSchemaUtils.toJavaDefaultValue(parsed.getField("c").get()));
    assertEquals(HoodieSchemaType.CHAR, parsed.getField("d").get().schema().getElementType().getType());
    assertEquals(HoodieSchemaType.INT8, parsed.getField("e").get().schema().getValueType().getType());
  }

  @Test
  void charEqualityIncludesLength() {
    assertEquals(HoodieSchema.createChar(10), HoodieSchema.createChar(10));
    assertEquals(HoodieSchema.createChar(10).hashCode(), HoodieSchema.createChar(10).hashCode());
    assertNotEquals(HoodieSchema.createChar(10), HoodieSchema.createChar(11));
    assertNotEquals(HoodieSchema.createChar(10), HoodieSchema.create(HoodieSchemaType.STRING));
    assertNotEquals(HoodieSchema.createInt8(), HoodieSchema.createInt16());
    assertNotEquals(HoodieSchema.createInt8(), HoodieSchema.create(HoodieSchemaType.INT));
    assertEquals(new HoodieSchema.CharLogicalType(4), new HoodieSchema.CharLogicalType(4));
    assertNotEquals(new HoodieSchema.CharLogicalType(4), new HoodieSchema.CharLogicalType(5));
  }

  @Test
  void hiveCharFormStaysString() {
    // Hive's AvroSerDe writes maxLength, not length: not Hudi's CHAR, so the schema is a STRING as before, with the
    // Hive properties kept for HiveTypeUtils
    HoodieSchema hiveChar = HoodieSchema.parse("{\"type\":\"string\",\"logicalType\":\"char\",\"maxLength\":10}");
    assertEquals(HoodieSchemaType.STRING, hiveChar.getType());
    assertNull(hiveChar.toAvroSchema().getLogicalType());
    assertEquals("char", hiveChar.getProp("logicalType"));
    assertEquals(10, hiveChar.getProp("maxLength"));
  }

  @Test
  void invalidLogicalTypesAreIgnored() {
    // the logical type on the wrong base type, or a non-positive length, is ignored by Avro: the base type is kept
    assertEquals(HoodieSchemaType.STRING, HoodieSchema.parse("{\"type\":\"string\",\"logicalType\":\"int8\"}").getType());
    assertEquals(HoodieSchemaType.LONG, HoodieSchema.parse("{\"type\":\"long\",\"logicalType\":\"int16\"}").getType());
    assertEquals(HoodieSchemaType.INT, HoodieSchema.parse("{\"type\":\"int\",\"logicalType\":\"char\",\"length\":3}").getType());
    assertEquals(HoodieSchemaType.STRING, HoodieSchema.parse("{\"type\":\"string\",\"logicalType\":\"char\",\"length\":0}").getType());
    assertEquals(HoodieSchemaType.STRING, HoodieSchema.parse("{\"type\":\"string\",\"logicalType\":\"char\",\"length\":\"3\"}").getType());
  }

  // -------------------------------------------------------------------------
  //  Degrade: readers that do not know the logical types
  // -------------------------------------------------------------------------

  /**
   * Avro 1.11 without Hudi's registrations, as an older Hudi or any other Avro reader has it: a fresh class loader
   * with only Avro and its dependencies, so {@code HoodieSchema}'s static {@code LogicalTypes.register} calls have
   * not run in it. The parser keeps the unknown {@code logicalType} as a plain property, without failing, and the
   * schema is the base type.
   */
  @ParameterizedTest
  @ValueSource(strings = {INT8_JSON, INT16_JSON, CHAR_10_JSON})
  void readerWithoutTheLogicalTypesDegradesToTheBaseType(String json) throws Exception {
    try (URLClassLoader avroOnly = avroOnlyClassLoader()) {
      Class<?> schemaClass = avroOnly.loadClass(Schema.class.getName());
      assertNotEquals(Schema.class, schemaClass, "the isolated loader must load its own Avro");
      Class<?> parserClass = avroOnly.loadClass(Schema.Parser.class.getName());
      Object parser = parserClass.getConstructor().newInstance();
      Object schema = parserClass.getMethod("parse", String.class).invoke(parser, json);

      assertNull(schemaClass.getMethod("getLogicalType").invoke(schema), "no logical type without the registration");
      String baseType = schemaClass.getMethod("getType").invoke(schema).toString();
      assertEquals(json.contains("\"string\"") ? "STRING" : "INT", baseType);
      Method getProp = schemaClass.getMethod("getProp", String.class);
      assertEquals(HoodieSchema.parse(json).getProp("logicalType"), getProp.invoke(schema, "logicalType"));
      // the property survives a write by the old reader, so the type comes back when Hudi reads it again
      String rewritten = schemaClass.getMethod("toString").invoke(schema).toString();
      assertEquals(json, rewritten);
      assertEquals(HoodieSchema.parse(json), HoodieSchema.parse(rewritten));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {INT8_JSON, INT16_JSON})
  void narrowIntDataIsReadableAsIntAndBack(String json) throws IOException {
    HoodieSchema narrow = recordOf(HoodieSchema.parse(json));
    HoodieSchema plain = recordOf(HoodieSchema.create(HoodieSchemaType.INT));
    GenericRecord written = new GenericData.Record(narrow.toAvroSchema());
    written.put("c", 42);
    // INT8 / INT16 values are Integers and their binary encoding is an Avro int: data written with either schema
    // reads with the other
    assertEquals(42, roundTrip(written, narrow.toAvroSchema(), plain.toAvroSchema()).get("c"));
    GenericRecord plainRecord = new GenericData.Record(plain.toAvroSchema());
    plainRecord.put("c", 42);
    assertEquals(42, roundTrip(plainRecord, plain.toAvroSchema(), narrow.toAvroSchema()).get("c"));
  }

  @Test
  void charDataKeepsItsPaddingAndIsReadableAsString() throws IOException {
    HoodieSchema charRecord = recordOf(HoodieSchema.createChar(5));
    HoodieSchema stringRecord = recordOf(HoodieSchema.create(HoodieSchemaType.STRING));
    GenericRecord written = new GenericData.Record(charRecord.toAvroSchema());
    written.put("c", "ab   ");
    assertEquals("ab   ", roundTrip(written, charRecord.toAvroSchema(), stringRecord.toAvroSchema()).get("c").toString());
    assertEquals("ab   ", roundTrip(written, charRecord.toAvroSchema(), charRecord.toAvroSchema()).get("c").toString());
  }

  // -------------------------------------------------------------------------
  //  Promotion, compatibility, projection, evolution
  // -------------------------------------------------------------------------

  @Test
  void promotionWidensNarrowInts() {
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT16, HoodieSchemaType.INT8));
    for (HoodieSchemaType wider : Arrays.asList(HoodieSchemaType.INT, HoodieSchemaType.LONG, HoodieSchemaType.FLOAT,
        HoodieSchemaType.DOUBLE, HoodieSchemaType.STRING)) {
      assertTrue(HoodieSchemaTypePromotion.canPromote(wider, HoodieSchemaType.INT8), wider + " <- INT8");
      assertTrue(HoodieSchemaTypePromotion.canPromote(wider, HoodieSchemaType.INT16), wider + " <- INT16");
    }
    // no narrowing
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT8, HoodieSchemaType.INT16));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT8, HoodieSchemaType.INT));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT16, HoodieSchemaType.INT));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT, HoodieSchemaType.LONG));
    // DATE and TIME are ints too, but not numbers
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT, HoodieSchemaType.DATE));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT8, HoodieSchemaType.DATE));
  }

  @Test
  void promotionTreatsCharAsString() {
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.STRING, HoodieSchemaType.CHAR));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.BYTES, HoodieSchemaType.CHAR));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.CHAR, HoodieSchemaType.CHAR));
    // STRING to CHAR is not a widening
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.CHAR, HoodieSchemaType.STRING));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.CHAR, HoodieSchemaType.BYTES));
  }

  @Test
  void readerWriterCompatibility() {
    HoodieSchema int8 = HoodieSchema.createInt8();
    HoodieSchema int16 = HoodieSchema.createInt16();
    HoodieSchema int32 = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema int64 = HoodieSchema.create(HoodieSchemaType.LONG);
    HoodieSchema string = HoodieSchema.create(HoodieSchemaType.STRING);

    assertCompatible(int8, int8);
    assertCompatible(int16, int8);
    assertCompatible(int32, int8);
    assertCompatible(int32, int16);
    assertCompatible(int64, int8);
    assertCompatible(HoodieSchema.create(HoodieSchemaType.DOUBLE), int16);
    assertCompatible(string, int8);
    assertIncompatible(int8, int32);
    assertIncompatible(int8, int16);
    assertIncompatible(int16, int32);
    assertIncompatible(int8, HoodieSchema.createDate());

    HoodieSchema char5 = HoodieSchema.createChar(5);
    HoodieSchema char10 = HoodieSchema.createChar(10);
    assertCompatible(char10, char10);
    // the values are strings whatever the declared lengths
    assertCompatible(char10, char5);
    assertCompatible(char5, char10);
    assertCompatible(string, char10);
    assertCompatible(HoodieSchema.create(HoodieSchemaType.BYTES), char10);
    // a CHAR reader decodes STRING data (compatibility only, like UUID over STRING)
    assertCompatible(char10, string);
    assertIncompatible(char10, int32);
    assertIncompatible(char10, HoodieSchema.create(HoodieSchemaType.BYTES));

    // nested and nullable
    assertCompatible(recordOf(HoodieSchema.createNullable(int64)), recordOf(int8));
    assertCompatible(recordOf(HoodieSchema.createArray(int32)), recordOf(HoodieSchema.createArray(int16)));
    assertIncompatible(recordOf(HoodieSchema.createMap(int8)), recordOf(HoodieSchema.createMap(int32)));
  }

  @Test
  void projectionAndEvolution() {
    HoodieSchema int8 = recordOf(HoodieSchema.createInt8());
    HoodieSchema int16 = recordOf(HoodieSchema.createInt16());
    HoodieSchema int32 = recordOf(HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema char5 = recordOf(HoodieSchema.createChar(5));
    HoodieSchema char10 = recordOf(HoodieSchema.createChar(10));
    HoodieSchema string = recordOf(HoodieSchema.create(HoodieSchemaType.STRING));

    assertTrue(HoodieSchemaCompatibility.isStrictProjectionOf(int8, int8));
    assertFalse(HoodieSchemaCompatibility.isStrictProjectionOf(int8, int32));
    assertTrue(HoodieSchemaCompatibility.isCompatibleProjectionOf(int8, int16));
    assertTrue(HoodieSchemaCompatibility.isCompatibleProjectionOf(int8, int32));
    assertFalse(HoodieSchemaCompatibility.isCompatibleProjectionOf(int32, int8));

    assertTrue(HoodieSchemaCompatibility.isStrictProjectionOf(char10, char10));
    assertFalse(HoodieSchemaCompatibility.isStrictProjectionOf(char5, char10));
    assertTrue(HoodieSchemaCompatibility.isCompatibleProjectionOf(char5, char10));
    assertFalse(HoodieSchemaCompatibility.isCompatibleProjectionOf(char10, char5));
    assertTrue(HoodieSchemaCompatibility.isCompatibleProjectionOf(char10, string));
    assertFalse(HoodieSchemaCompatibility.isCompatibleProjectionOf(string, char10));

    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(char10, recordOf(HoodieSchema.createChar(10))));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(char5, char10));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(int8, int32));

    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(int8, recordOf(HoodieSchema.createInt8())));
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(int16, recordOf(HoodieSchema.createInt16())));
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(int8, int16));
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(int8, int32));
    assertTrue(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(char10, recordOf(HoodieSchema.createChar(10))));
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(char5, char10));
    assertFalse(HoodieSchemaComparatorForSchemaEvolution.schemaEquals(char10, string));

    // new reads previous: INT8 evolves to INT16 and INT16 to INT (widening), INT does not go back to INT8
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(int8, int16));
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(int16, int32));
    assertFalse(HoodieSchemaCompatibility.isSchemaCompatible(int32, int8));
  }

  @Test
  void equivalenceComparesCharLength() {
    HoodieSchema char5 = recordOf(HoodieSchema.createChar(5));
    HoodieSchema char10 = recordOf(HoodieSchema.createChar(10));
    assertEquals(Collections.singletonList("$.c: expected char length 5 but was 10"),
        HoodieSchemaEquivalence.differences(char5, char10, HoodieSchemaEquivalence.Mode.LOGICAL));
    assertTrue(HoodieSchemaEquivalence.equivalent(char10, recordOf(HoodieSchema.createChar(10)), HoodieSchemaEquivalence.Mode.PHYSICAL));
    assertFalse(HoodieSchemaEquivalence.equivalent(recordOf(HoodieSchema.createInt8()), recordOf(HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaEquivalence.Mode.LOGICAL));
  }

  // -------------------------------------------------------------------------
  //  Value handling sites: INT8 / INT16 as INT, CHAR as STRING
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @EnumSource(value = HoodieSchemaType.class, names = {"INT8", "INT16"})
  void defaultValuesOfNarrowIntsAreInts(HoodieSchemaType type) {
    HoodieSchemaField field = HoodieSchemaField.of("c", HoodieSchema.create(type), null, 7);
    assertEquals(7, HoodieSchemaUtils.toJavaDefaultValue(field));
  }

  @Test
  void internalSchemaReadsNarrowIntsAsIntAndCharAsString() {
    HoodieSchema record = HoodieSchema.createRecord("r", "ns", null, Arrays.asList(
        HoodieSchemaField.of("a", HoodieSchema.createInt8()),
        HoodieSchemaField.of("b", HoodieSchema.createInt16()),
        HoodieSchemaField.of("c", HoodieSchema.createChar(4))));
    Types.RecordType internal = InternalSchemaConverter.convert(record).getRecord();
    assertEquals(Types.IntType.get(), internal.fieldType("a"));
    assertEquals(Types.IntType.get(), internal.fieldType("b"));
    assertEquals(Types.StringType.get(), internal.fieldType("c"));
    // InternalSchema has no narrow ints or CHAR (RFC-104 retires it): the round trip is lossy, values unchanged
    HoodieSchema back = InternalSchemaConverter.convert(InternalSchemaConverter.convert(record), "ns.r");
    assertEquals(HoodieSchemaType.INT, back.getField("a").get().getNonNullSchema().getType());
    assertEquals(HoodieSchemaType.STRING, back.getField("c").get().getNonNullSchema().getType());
  }

  @Test
  void partitionSchemasWithNewTypesAreParseable() {
    for (HoodieSchema type : Arrays.asList(HoodieSchema.createInt8(), HoodieSchema.createInt16(), HoodieSchema.createChar(3))) {
      assertTrue(Conversions.isPartitionSchemaSupportedConversion(recordOf(type)), type.toString());
    }
  }

  @Test
  void logicalTypeNamesAreKnownToAvroOnlyAfterRegistration() {
    // Registration is global and happens when HoodieSchema is loaded, like VARIANT, BLOB and VECTOR
    Set<String> names = new LinkedHashSet<>(LogicalTypes.getCustomRegisteredTypes().keySet());
    assertTrue(names.containsAll(Arrays.asList("int8", "int16", "char", "variant", "blob", "vector")), names.toString());
  }

  private static void assertCompatible(HoodieSchema reader, HoodieSchema writer) {
    assertEquals(HoodieSchemaCompatibilityChecker.SchemaCompatibilityType.COMPATIBLE,
        HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(reader, writer, true).getType(),
        "reader " + reader + " / writer " + writer);
  }

  private static void assertIncompatible(HoodieSchema reader, HoodieSchema writer) {
    assertEquals(HoodieSchemaCompatibilityChecker.SchemaCompatibilityType.INCOMPATIBLE,
        HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(reader, writer, true).getType(),
        "reader " + reader + " / writer " + writer);
  }

  private static HoodieSchema recordOf(HoodieSchema column) {
    return HoodieSchema.createRecord("r", "ns", null, Collections.singletonList(HoodieSchemaField.of("c", column)));
  }

  private static GenericRecord roundTrip(GenericRecord record, Schema writer, Schema reader) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(writer).write(record, encoder);
    encoder.flush();
    return new GenericDatumReader<GenericRecord>(writer, reader).read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
  }

  /**
   * A class loader with Avro and the libraries {@code Schema} needs (Jackson, SLF4J API) only, over the bootstrap
   * loader, so nothing of Hudi is visible in it.
   */
  private static URLClassLoader avroOnlyClassLoader() throws ClassNotFoundException {
    Set<URL> urls = new LinkedHashSet<>();
    for (String className : Arrays.asList("org.apache.avro.Schema", "com.fasterxml.jackson.core.JsonFactory",
        "com.fasterxml.jackson.databind.ObjectMapper", "com.fasterxml.jackson.annotation.JsonProperty", "org.slf4j.Logger")) {
      urls.add(Class.forName(className).getProtectionDomain().getCodeSource().getLocation());
    }
    return new URLClassLoader(urls.toArray(new URL[0]), null);
  }
}
