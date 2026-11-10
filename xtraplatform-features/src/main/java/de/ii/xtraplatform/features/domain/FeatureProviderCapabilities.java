/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FeatureProviderCapabilities {

  public enum Level {
    MINIMAL,
    BASIC,
    DEFAULT,
    FULL
  }

  public enum Operation {
    PAGING,
    COUNTING,
    SORTING
  }

  public enum Cql2Operator {
    ID_IN,
    EQUALS,
    LIKE,
    S_INTERSECTS,
    T_INTERSECTS
  }

  public enum Cql2Class {
    BASIC,
    ADVANCED,
    CASE_I,
    ACCENT_I,
    BASIC_SPATIAL,
    SPATIAL,
    TEMPORAL,
    ARRAY,
    PROP_PROP,
    FUNCTIONS,
    ARITHMETIC
  }

  private static final Profile PROFILE_MINIMAL =
      ImmutableProfile.builder()
          .level(Level.MINIMAL)
          .addQueryOps(Operation.PAGING)
          .addCql2Operators(Cql2Operator.ID_IN)
          .build();
  private static final Profile PROFILE_BASIC =
      ImmutableProfile.builder()
          .from(PROFILE_MINIMAL)
          .level(Level.BASIC)
          .addCql2Operators(Cql2Operator.EQUALS, Cql2Operator.S_INTERSECTS)
          .build();
  private static final Profile PROFILE_DEFAULT =
      ImmutableProfile.builder()
          .from(PROFILE_BASIC)
          .level(Level.DEFAULT)
          .addQueryOps(Operation.COUNTING)
          .addCql2Operators(Cql2Operator.LIKE, Cql2Operator.T_INTERSECTS)
          .build();
  private static final Profile PROFILE_FULL =
      ImmutableProfile.builder()
          .from(PROFILE_DEFAULT)
          .level(Level.FULL)
          .addQueryOps(Operation.SORTING)
          .addCql2Classes()
          .build();
  private static final Map<Level, Profile> PROFILES =
      Map.of(
          Level.MINIMAL, PROFILE_MINIMAL,
          Level.BASIC, PROFILE_BASIC,
          Level.DEFAULT, PROFILE_DEFAULT,
          Level.FULL, PROFILE_FULL);

  @Value.Immutable
  public interface Profile {

    Level getLevel();

    List<Operation> getQueryOps();

    List<Cql2Operator> getCql2Operators();

    List<Cql2Class> getCql2Classes();
  }

  public abstract Level getLevel();

  protected abstract List<Operation> getAdditionalQueryOps();

  protected abstract List<Cql2Operator> getAdditionalCql2Operators();

  protected abstract List<Cql2Class> getAdditionalCql2Classes();

  @Value.Derived
  public List<Operation> getQueryOps() {
    return Stream.concat(
            PROFILES.get(getLevel()).getQueryOps().stream(), getAdditionalQueryOps().stream())
        .collect(Collectors.toList());
  }

  @Value.Derived
  public List<Cql2Operator> getCql2Operators() {
    return Stream.concat(
            PROFILES.get(getLevel()).getCql2Operators().stream(),
            getAdditionalCql2Operators().stream())
        .collect(Collectors.toList());
  }

  @Value.Derived
  public List<Cql2Class> getCql2Classes() {
    return Stream.concat(
            PROFILES.get(getLevel()).getCql2Classes().stream(), getAdditionalCql2Classes().stream())
        .collect(Collectors.toList());
  }

  public boolean supportsQueryOp(Operation queryOps) {
    return getQueryOps().contains(queryOps);
  }

  public boolean supportsCql2Operator(Cql2Operator cql2Operator) {
    return getCql2Operators().contains(cql2Operator);
  }

  public boolean supportsCql2Class(Cql2Class cql2Class) {
    return getCql2Classes().contains(cql2Class);
  }
}
