/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.sql.domain.SqlPath.JoinType;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableSqlRelation.Builder.class)
public interface SqlRelation {

  enum CARDINALITY {
    ONE_2_ONE,
    ONE_2_N,
    M_2_N
  }

  CARDINALITY getCardinality();

  String getSourceContainer();

  String getSourceField();

  Optional<String> getSourcePrimaryKey();

  Optional<String> getSourceSortKey();

  Optional<String> getSourceFilter();

  String getTargetContainer();

  String getTargetField();

  Optional<String> getTargetFilter();

  Optional<String> getJunction();

  Optional<String> getJunctionSource();

  Optional<String> getJunctionTarget();

  Optional<String> getJunctionFilter();

  @Value.Default
  default JoinType getJoinType() {
    return JoinType.INNER;
  }

  @Value.Check
  default void check() {
    Preconditions.checkState(
        (getCardinality() == CARDINALITY.M_2_N && getJunction().isPresent())
            || (!getJunction().isPresent()),
        "when a junction is set, cardinality needs to be M_2_N, when no junction is set, cardinality is not allowed to be M_2_N");
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isOne2One() {
    return getCardinality() == CARDINALITY.ONE_2_ONE;
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isOne2N() {
    return getCardinality() == CARDINALITY.ONE_2_N;
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isM2N() {
    return getCardinality() == CARDINALITY.M_2_N;
  }

  @JsonIgnore
  @Value.Derived
  default List<String> asPath() {
    if (isM2N()) {
      return ImmutableList.of(
          String.format(
              "[%s=%s]%s%s",
              getSourceField(),
              getJunctionSource().get(),
              getJunction().get(),
              getJunctionFilter().map(filter -> "{filter=" + filter + "}").orElse("")),
          String.format(
              "[%s=%s]%s%s",
              getJunctionTarget().get(),
              getTargetField(),
              getTargetContainer(),
              getTargetFilter().map(filter -> "{filter=" + filter + "}").orElse("")));
    }

    return ImmutableList.of(
        String.format(
            "[%s=%s]%s%s",
            getSourceField(),
            getTargetField(),
            getTargetContainer(),
            getTargetFilter().map(filter -> "{filter=" + filter + "}").orElse("")));
  }
}
