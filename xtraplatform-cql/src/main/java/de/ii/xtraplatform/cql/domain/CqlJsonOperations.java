/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CQL2 JSON 1.0 encodes operators, CASEI, ACCENTI and functions alike as {"op": name, "args":
 * [...]}. An "op" that is not one of the operators is a function.
 */
final class CqlJsonOperations {

  static final String OP = "op";
  static final String ARGS = "args";

  private static final Set<String> OPERATIONS =
      Arrays.stream(Operation.class.getAnnotation(JsonSubTypes.class).value())
          .map(type -> type.name().toLowerCase(Locale.ROOT))
          .collect(Collectors.toUnmodifiableSet());

  private CqlJsonOperations() {}

  static boolean isOperation(String op) {
    return OPERATIONS.contains(op.toLowerCase(Locale.ROOT));
  }

  static boolean isFunction(JsonNode node) {
    JsonNode op = node.get(OP);
    return Objects.nonNull(op)
        && op.isTextual()
        && !isOperation(op.textValue())
        && !Casei.TYPE.equalsIgnoreCase(op.textValue())
        && !Accenti.TYPE.equalsIgnoreCase(op.textValue());
  }

  static JsonNode args(JsonParser parser, String op, JsonNode node) throws JsonParseException {
    JsonNode args = node.get(ARGS);
    if (Objects.isNull(args) || !args.isArray()) {
      throw new JsonParseException(
          parser, String.format("'%s' requires an array of arguments.", op));
    }
    return args;
  }

  static Operand singleArg(JsonParser parser, String op, List<Operand> args)
      throws JsonParseException {
    if (args.size() != 1) {
      throw new JsonParseException(
          parser, String.format("'%s' requires exactly one argument.", op));
    }
    return args.get(0);
  }

  static void write(JsonGenerator gen, String op, List<?> args) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(OP, op);
    gen.writeFieldName(ARGS);
    gen.writeStartArray();
    for (Object arg : args) {
      gen.writeObject(arg);
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }
}
