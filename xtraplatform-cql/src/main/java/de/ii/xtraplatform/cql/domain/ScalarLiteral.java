/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableScalarLiteral.Builder.class)
public interface ScalarLiteral extends Scalar, Literal, CqlNode {

  static ScalarLiteral of(Double literal) {
    return new Builder(literal).build();
  }

  static ScalarLiteral of(Integer literal) {
    return new Builder(literal).build();
  }

  static ScalarLiteral of(Long literal) {
    return new Builder(literal).build();
  }

  static ScalarLiteral of(Boolean literal) {
    return new Builder(literal).build();
  }

  static ScalarLiteral of(String literal) {
    return new Builder(literal).build();
  }

  static ScalarLiteral of(String literal, boolean determineType) {
    return new Builder(literal, determineType).build();
  }

  class Builder extends ImmutableScalarLiteral.Builder {
    public Builder() {
      super();
    }

    @JsonCreator
    public Builder(Double literal) {
      super();
      value(literal);
      type(Double.class);
    }

    @JsonCreator
    public Builder(Integer literal) {
      super();
      value(literal);
      type(Integer.class);
    }

    @JsonCreator
    public Builder(Long literal) {
      super();
      value(literal);
      type(Long.class);
    }

    @JsonCreator
    public Builder(Boolean literal) {
      super();
      value(literal);
      type(Boolean.class);
    }

    @JsonCreator
    public Builder(String literal) {
      this(literal, false);
    }

    public Builder(String literal, boolean determineType) {
      super();
      if (determineType) {
        Object castedLiteral = castToType(literal);
        value(castedLiteral);
        type(castedLiteral.getClass());
      } else {
        value(literal);
        type(String.class);
      }
    }

    private Object castToType(String literal) {
      try {
        return Integer.valueOf(literal);
      } catch (NumberFormatException e) {
        try {
          return Long.valueOf(literal);
        } catch (NumberFormatException e2) {
          try {
            return Double.valueOf(literal);
          } catch (NumberFormatException e3) {
            if ("true".equalsIgnoreCase(literal) || "false".equalsIgnoreCase(literal)) {
              return Boolean.valueOf(literal);
            }
          }
        }
      }

      return literal;
    }
  }
}
