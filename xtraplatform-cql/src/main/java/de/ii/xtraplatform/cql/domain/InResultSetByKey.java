/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.immutables.value.Value;

/**
 * Predicate that tests whether the composite key of a feature is contained in a named result set
 * that is defined by another query of the same query expression.
 *
 * <p>Unlike {@link InResultSet}, which compares a single value, the key of this predicate has
 * several parts. The parts are matched by name, not by position:
 *
 * <pre>{@code
 * {
 *   "op": "inResultSetByKey",
 *   "args": [
 *     { "land": {"property": "gkz_lan"}, "kreis": {"property": "gkz_krs"} },
 *     "fs_gemeinde"
 *   ]
 * }
 * }</pre>
 *
 * <p>The producing query declares the same part names for its own properties, so a transposition of
 * two parts of the same type — which a positional key could not detect — cannot occur, and a part
 * that neither side declares is reported before the query is encoded.
 *
 * <p>The parts are held in a canonical order (sorted by part name) so that the row constructor of
 * the consumer and the projection of the producer are built in the same order without the order in
 * the request mattering.
 *
 * <p>This predicate has no CQL2-Text encoding; it can only be used in a query expression.
 */
@Value.Immutable
@JsonDeserialize(using = InResultSetByKey.Deserializer.class)
@JsonSerialize(using = InResultSetByKey.Serializer.class)
public interface InResultSetByKey extends BinaryScalarOperation, ResultSetReference {

  String TYPE = "inResultSetByKey";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  /** Names of the key parts, in the same order as the properties in the first argument. */
  List<String> getKeyNames();

  /** Feature type of the query that defines the result set. */
  @JsonIgnore
  Optional<String> getProducerType();

  /** Effective filter of the query that defines the result set. */
  @JsonIgnore
  Optional<Cql2Expression> getProducerFilter();

  /**
   * Properties of the producing feature type that form the key, by part name. Set by the service
   * when the result-set reference is resolved; the part names are the same as {@link
   * #getKeyNames()}.
   */
  @JsonIgnore
  Map<String, String> getProducerKey();

  /**
   * Key tuples of the result set, materialized by the service before the filter is encoded. Each
   * element holds the part values in the canonical part order. When present, the predicate is
   * encoded as a literal row-constructor {@code IN} list; an empty list means the result set has no
   * members.
   */
  @JsonIgnore
  Optional<List<List<Object>>> getMaterializedValues();

  /**
   * Name of a table the service has materialized the result set into, with one column per key part.
   * Used for sets that are too large to inline as a literal list.
   */
  @JsonIgnore
  Optional<String> getMaterializedTable();

  @JsonIgnore
  @Value.Lazy
  default String getSetName() {
    return String.valueOf(((ScalarLiteral) getArgs().get(1)).getValue());
  }

  /** Properties of the consuming feature type that form the key, in canonical part order. */
  @SuppressWarnings("unchecked")
  @JsonIgnore
  @Value.Lazy
  default List<Property> getProperties() {
    return ((List<Scalar>) ((ArrayLiteral) getArgs().get(0)).getValue())
        .stream().map(Property.class::cast).collect(ImmutableList.toImmutableList());
  }

  /** Properties of the consuming feature type that form the key, by part name. */
  @JsonIgnore
  @Value.Lazy
  default Map<String, Property> getKey() {
    Map<String, Property> key = new LinkedHashMap<>();
    List<Property> properties = getProperties();
    for (int i = 0; i < getKeyNames().size(); i++) {
      key.put(getKeyNames().get(i), properties.get(i));
    }
    return key;
  }

  @Value.Check
  default void checkArgs() {
    Preconditions.checkState(
        getArgs().size() == 2 && getArgs().get(0) instanceof ArrayLiteral,
        "the first argument of %s must be the key, found: %s",
        TYPE,
        getArgs());
    Preconditions.checkState(
        getArgs().get(1) instanceof ScalarLiteral
            && ((ScalarLiteral) getArgs().get(1)).getType() == String.class,
        "the second argument of %s must be the name of a result set, found: %s",
        TYPE,
        getArgs().get(1));
    Object elements = ((ArrayLiteral) getArgs().get(0)).getValue();
    Preconditions.checkState(
        elements instanceof List && !((List<?>) elements).isEmpty(),
        "the key of %s must have at least one part",
        TYPE);
    Preconditions.checkState(
        ((List<?>) elements).stream().allMatch(e -> e instanceof Property),
        "every part of the key of %s must be a property, found: %s",
        TYPE,
        elements);
    Preconditions.checkState(
        getKeyNames().size() == ((List<?>) elements).size(),
        "the key of %s has %s parts, but %s part names",
        TYPE,
        ((List<?>) elements).size(),
        getKeyNames().size());
  }

  /**
   * Builds the predicate from the key parts of the consuming feature type. The parts are sorted by
   * name, so two predicates that declare the same parts in a different order are equal.
   */
  static InResultSetByKey of(Map<String, Property> key, String setName) {
    Map<String, Property> sorted = new TreeMap<>(key);

    return new ImmutableInResultSetByKey.Builder()
        .args(
            ImmutableList.of(
                ArrayLiteral.of(ImmutableList.<Scalar>copyOf(sorted.values())),
                ScalarLiteral.of(setName)))
        .keyNames(sorted.keySet())
        .build();
  }

  abstract class Builder extends BinaryScalarOperation.Builder<InResultSetByKey> {}

  class Deserializer extends StdDeserializer<InResultSetByKey> {

    private static final long serialVersionUID = 1L;

    protected Deserializer() {
      this(null);
    }

    protected Deserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public InResultSetByKey deserialize(JsonParser parser, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = parser.getCodec().readTree(parser);
      JsonNode args = node.get("args");

      if (args == null || !args.isArray() || args.size() != 2) {
        throw new JsonParseException(
            parser, String.format("%s must have two arguments, the key and a result set.", TYPE));
      }
      if (!args.get(0).isObject()) {
        throw new JsonParseException(
            parser,
            String.format(
                "The first argument of %s must be the key, an object with one member per key part.",
                TYPE));
      }
      if (!args.get(1).isTextual()) {
        throw new JsonParseException(
            parser,
            String.format("The second argument of %s must be the name of a result set.", TYPE));
      }

      Map<String, Property> key = new LinkedHashMap<>();
      Iterator<Map.Entry<String, JsonNode>> parts = args.get(0).fields();
      while (parts.hasNext()) {
        Map.Entry<String, JsonNode> part = parts.next();
        if (!part.getValue().isObject() || part.getValue().get("property") == null) {
          throw new JsonParseException(
              parser,
              String.format("The key part '%s' of %s must be a property.", part.getKey(), TYPE));
        }
        key.put(part.getKey(), parser.getCodec().treeToValue(part.getValue(), Property.class));
      }
      if (key.isEmpty()) {
        throw new JsonParseException(
            parser, String.format("The key of %s must have at least one part.", TYPE));
      }

      return InResultSetByKey.of(key, args.get(1).asText());
    }
  }

  class Serializer extends StdSerializer<InResultSetByKey> {

    private static final long serialVersionUID = 1L;

    protected Serializer() {
      this(null);
    }

    protected Serializer(Class<InResultSetByKey> t) {
      super(t);
    }

    @Override
    public void serialize(InResultSetByKey value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("op", value.getOp());
      gen.writeFieldName("args");
      gen.writeStartArray();
      gen.writeStartObject();
      for (Map.Entry<String, Property> part : value.getKey().entrySet()) {
        gen.writeFieldName(part.getKey());
        gen.writeObject(part.getValue());
      }
      gen.writeEndObject();
      gen.writeString(value.getSetName());
      gen.writeEndArray();
      gen.writeEndObject();
    }

    // the op is written above; with EXISTING_PROPERTY there is no separate type id to add
    @Override
    public void serializeWithType(
        InResultSetByKey value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSerializer)
        throws IOException {
      serialize(value, gen, serializers);
    }
  }
}
