/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;

@JsonDeserialize(using = Cql2Expression.Cql2JsonDeserializer.class)
public interface Cql2Expression extends Operand, CqlNode {

  class Cql2JsonDeserializer extends StdDeserializer<Cql2Expression> {

    protected Cql2JsonDeserializer() {
      this(null);
    }

    protected Cql2JsonDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Cql2Expression deserialize(JsonParser parser, DeserializationContext ctxt)
        throws IOException, JsonMappingException {

      JsonNode node = parser.getCodec().readTree(parser);

      if (node.isBoolean()) {
        return BooleanValue2.of(node.asBoolean());
      }

      try {
        return parser.getCodec().treeToValue(node, Operation.class);
      } catch (IOException e) {
        throw new CqlParseException(e.getMessage());
      }
    }
  }

  default <T> T accept(CqlVisitor<T> visitor, boolean isRoot) {
    T visited = accept(visitor);
    return isRoot ? visitor.postProcess(this, visited) : visited;
  }
}
