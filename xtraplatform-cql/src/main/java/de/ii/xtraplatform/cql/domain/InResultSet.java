/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Predicate that tests whether a property value (or the feature id) is contained in a named result
 * set that is defined by another query of the same query expression.
 *
 * <p>The two arguments are the property and the name of the result set (a string literal). The
 * predicate behaves like an IN expression (single values) or an A_OVERLAPS expression (arrays)
 * against the object ids in the result set.
 *
 * <p>The result-set reference is resolved by the service before the filter is encoded: the
 * producing query's feature type and its effective filter are attached to this node. They are not
 * part of the JSON or text encoding.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableInResultSet.Builder.class)
public interface InResultSet extends BinaryScalarOperation {

  String TYPE = "inResultSet";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  /** Feature type of the query that defines the result set. */
  @JsonIgnore
  Optional<String> getProducerType();

  /** Effective filter of the query that defines the result set. */
  @JsonIgnore
  Optional<Cql2Expression> getProducerFilter();

  /**
   * Property of the producing feature type whose values form the result set (projected result set).
   * If empty, the result set consists of the ids of the selected features.
   */
  @JsonIgnore
  Optional<String> getProducerValues();

  /**
   * Values of the result set, materialized by the service before the filter is encoded. When
   * present, the predicate is encoded as a literal {@code IN} list rather than a nested subquery;
   * an empty list means the result set has no members. When absent, the result set is re-derived
   * inline (subquery / CTE).
   */
  @JsonIgnore
  Optional<List<Object>> getMaterializedValues();

  /**
   * Name of a table the service has materialized the result set into (one column of member values).
   * Used for sets that are too large to inline as a literal list: the predicate is encoded as
   * {@code IN (SELECT <value column> FROM <table>)} against the pre-materialized, indexed table, so
   * the producing query runs once instead of being re-derived in every consumer.
   */
  @JsonIgnore
  Optional<String> getMaterializedTable();

  @JsonIgnore
  @Value.Lazy
  default String getSetName() {
    return String.valueOf(((ScalarLiteral) getArgs().get(1)).getValue());
  }

  @Value.Check
  default void checkArgs() {
    Preconditions.checkState(
        getArgs().size() == 2 && getArgs().get(0) instanceof Property,
        "the first argument of %s must be a property, found: %s",
        TYPE,
        getArgs());
    Preconditions.checkState(
        getArgs().get(1) instanceof ScalarLiteral
            && ((ScalarLiteral) getArgs().get(1)).getType() == String.class,
        "the second argument of %s must be the name of a result set, found: %s",
        TYPE,
        getArgs().get(1));
  }

  static InResultSet of(String property, String setName) {
    return new ImmutableInResultSet.Builder()
        .args(ImmutableList.of(Property.of(property), ScalarLiteral.of(setName)))
        .build();
  }

  static InResultSet of(Property property, String setName) {
    return new ImmutableInResultSet.Builder()
        .args(ImmutableList.of(property, ScalarLiteral.of(setName)))
        .build();
  }

  abstract class Builder extends BinaryScalarOperation.Builder<InResultSet> {}
}
