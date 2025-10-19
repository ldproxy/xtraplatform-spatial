/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.util.LambdaWithException;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.transcode.json.GeometryDecoderJson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonDeserialize(using = Operand.OperandDeserializer.class)
public interface Operand extends CqlNode {

  class OperandDeserializer extends StdDeserializer<Operand> {

    private static final List<String> ARRAY =
        Arrays.stream(ArrayFunction.values()).map(op -> op.toString().toLowerCase()).toList();
    private static final List<String> SPATIAL =
        Arrays.stream(SpatialFunction.values()).map(op -> op.toString().toLowerCase()).toList();
    private static final List<String> TEMPORAL =
        Arrays.stream(TemporalFunction.values()).map(op -> op.toString().toLowerCase()).toList();
    private static final List<String> SCALAR =
        ImmutableList.of(
            "value", "list", "args", "eq", "neq", "gt", "gte", "lt", "lte", "between", "in",
            "isnull");

    protected OperandDeserializer() {
      this(null);
    }

    protected OperandDeserializer(Class<?> vc) {
      super(vc);
    }

    private Scalar getScalar(JsonNode node) {
      if (node.isBoolean()) return ScalarLiteral.of(node.asBoolean());
      if (node.isInt()) return ScalarLiteral.of(node.asInt());
      if (node.isLong()) return ScalarLiteral.of(node.asLong());
      if (node.isDouble()) return ScalarLiteral.of(node.asDouble());

      return ScalarLiteral.of(node.asText());
    }

    private Operand getOperand(JsonParser parser, JsonNode node, String parent)
        throws JsonProcessingException {
      ObjectCodec oc = parser.getCodec();
      if (node.isObject()) {
        if (Objects.nonNull(node.get("property"))) {
          return oc.treeToValue(node, Property.class);
        } else if (Objects.nonNull(node.get("date"))) {
          return oc.treeToValue(node.get("date"), TemporalLiteral.class);
        } else if (Objects.nonNull(node.get("timestamp"))) {
          return oc.treeToValue(node.get("timestamp"), TemporalLiteral.class);
        } else if (Objects.nonNull(node.get("interval"))) {
          if (node.get("interval").isArray()) {
            Temporal op1 = (Temporal) getOperand(parser, node.get("interval").get(0), parent);
            Temporal op2 = (Temporal) getOperand(parser, node.get("interval").get(1), parent);

            return TemporalLiteral.interval(op1, op2);
          }
          throw new JsonParseException(parser, "Interval has to be an array.");
        } else if (Objects.nonNull(node.get("bbox"))) {
          return SpatialLiteral.of(oc.treeToValue(node, Bbox.class));
        } else if (Objects.nonNull(node.get("type"))) {
          final Optional<EpsgCrs> filterCrs = getFilterCrs(oc);
          try {
            return SpatialLiteral.of(
                new GeometryDecoderJson().decode(node, filterCrs, Optional.empty()));
          } catch (IOException e) {
            throw new IllegalArgumentException("Error parsing CQL2-JSON geometry.", e);
          }
        } else if (Objects.nonNull(node.get("casei"))) {
          return Casei.of(getOperand(parser, node.get("casei"), parent));
        } else if (Objects.nonNull(node.get("accenti"))) {
          return Accenti.of(getOperand(parser, node.get("accenti"), parent));
        } else if (Objects.nonNull(node.get("op"))) {
          return oc.treeToValue(node, Operation.class);
        } else if (Objects.nonNull(node.get("function"))) {
          List<Operand> list = new ArrayList<>();
          Iterator<JsonNode> iterator = node.get("function").get("args").elements();
          while (iterator.hasNext()) {
            JsonNode listNode = iterator.next();
            list.add(getOperand(parser, listNode, "args"));
          }
          return Function.of(node.get("function").get("name").textValue(), list);
        } else if (Objects.nonNull(node.get("$parameter"))) {
          return oc.treeToValue(node.get("$parameter"), Parameter.class);
        }
      } else if (node.isArray()) {
        if (TEMPORAL.contains(parent)) {
          return oc.treeToValue(node, TemporalLiteral.class);
        }

        List<Scalar> scalars =
            StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                        ((ArrayNode) node).elements(), Spliterator.ORDERED),
                    false)
                .map(
                    LambdaWithException.mayThrow(
                        jsonNode -> (Scalar) getOperand(parser, jsonNode, parent)))
                .collect(Collectors.toList());

        return ArrayLiteral.of(scalars);
      } else if (node.isValueNode()) {
        // we have to guess, try temporal first
        try {
          return oc.treeToValue(node, TemporalLiteral.class);
        } catch (JsonProcessingException e) {
          return getScalar(node);
        }
      }

      throw new JsonParseException(
          parser,
          String.format("Unexpected operand of type %s in member %s.", node.getNodeType(), parent));
    }

    @Override
    public Operand deserialize(JsonParser parser, DeserializationContext ctxt)
        throws IOException, JsonMappingException {

      // Parse "object" node into Jackson's tree model
      JsonNode node = parser.getCodec().readTree(parser);

      JsonStreamContext parent = parser.getParsingContext().getParent();

      // Get name of the parent key
      String parentName = Objects.requireNonNullElse(parent.getCurrentName(), "null").toLowerCase();

      return getOperand(parser, node, parentName);
    }

    private Optional<EpsgCrs> getFilterCrs(ObjectCodec oc) throws JsonMappingException {
      InjectableValues iv = ((ObjectMapper) oc).getInjectableValues();
      if (Objects.nonNull(iv)) {
        Object value = iv.findInjectableValue("filterCrs", null, null, null);
        if (value instanceof EpsgCrs) {
          return Optional.of((EpsgCrs) value);
        }
      }
      return Optional.of(OgcCrs.CRS84);
    }
  }
}
