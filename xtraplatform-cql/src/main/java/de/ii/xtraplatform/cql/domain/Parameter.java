/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import de.ii.xtraplatform.jsonschema.domain.ImmutableJsonSchemaRef;
import de.ii.xtraplatform.jsonschema.domain.JsonSchema;
import java.io.IOException;
import java.util.Map.Entry;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(of = "new")
@JsonSerialize(using = Parameter.ParameterSerializer.class)
@JsonDeserialize(using = Parameter.ParameterDeserializer.class)
public interface Parameter extends Scalar, Spatial, Temporal, Operand, Vector, CqlNode {

  static Parameter of(String name, JsonSchema schema) {
    return ImmutableParameter.builder().name(name).schema(schema).build();
  }

  @Value.Parameter
  String getName();

  @Value.Parameter
  JsonSchema getSchema();

  class ParameterDeserializer extends StdDeserializer<Parameter> {

    protected ParameterDeserializer() {
      this(null);
    }

    protected ParameterDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Parameter deserialize(JsonParser parser, DeserializationContext ctxt)
        throws IOException, JsonMappingException {

      JsonNode node = parser.getCodec().readTree(parser);

      // Using '$ref' directly inside '$parameter' is deprecated.
      // TODO: Remove support in future versions.
      if (node.has("$ref")) {
        JsonNode value = node.get("$ref");
        if (!value.isTextual()) {
          throw new JsonParseException(
              parser, "The value of '$ref' must be a string, found: " + value.getNodeType());
        }
        String name = value.asText();
        if (name.startsWith("#/parameters/")) {
          name = name.substring("#/parameters/".length());
        }
        return Parameter.of(name, new ImmutableJsonSchemaRef.Builder().ref(value.asText()).build());
      }

      if (!node.isObject()) {
        throw new JsonParseException(
            parser,
            String.format(
                "Expected parameter to be an object with a single member. Found: %s.",
                node.getNodeType()));
      }

      if (node.size() != 1) {
        throw new JsonParseException(
            parser,
            String.format(
                "Expected parameter to be an object with a single member. Found: %s.",
                node.getNodeType()));
      }

      Entry<String, JsonNode> param = node.properties().iterator().next();

      return Parameter.of(
          param.getKey(), parser.getCodec().treeToValue(param.getValue(), JsonSchema.class));
    }
  }

  class ParameterSerializer extends StdSerializer<Parameter> {

    protected ParameterSerializer() {
      this(null);
    }

    protected ParameterSerializer(Class<Parameter> t) {
      super(t);
    }

    @Override
    public void serialize(
        Parameter param, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("$parameter");
      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField(param.getName(), param.getSchema());
      jsonGenerator.writeEndObject();
      jsonGenerator.writeEndObject();
    }
  }
}
