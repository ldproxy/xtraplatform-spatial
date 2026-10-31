/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableFeatureProperty.Builder.class)
public interface FeatureProperty extends Buildable<FeatureProperty> {

  // NOTE: Role with ID, SPATIAL, TEMPORAL, REFERENCE, REFERENCE_EMBED
  // NOTE: more specific types, in addition or instead of Type
  enum Role {
    ID,
    TYPE,
    PRIMARY_GEOMETRY,
    PRIMARY_INSTANT,
    PRIMARY_INTERVAL_START,
    PRIMARY_INTERVAL_END,
    SECONDARY_GEOMETRY
  }

  enum Type {
    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    DATETIME,
    DATE,
    GEOMETRY,
    OBJECT,
    VALUE_ARRAY,
    OBJECT_ARRAY,
    UNKNOWN
  }

  abstract class Builder implements BuildableBuilder<FeatureProperty> {}

  @Override
  default ImmutableFeatureProperty.Builder getBuilder() {
    return new ImmutableFeatureProperty.Builder().from(this);
  }

  // @Nullable
  @JsonIgnore
  String getName();

  String getPath();

  @Value.Default
  default Type getType() {
    return Type.STRING;
  }

  Optional<Role> getRole();

  Optional<String> getConstantValue();

  Map<String, String> getAdditionalInfo();

  // TODO
  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isId() {
    return getRole().filter(role -> role == Role.ID).isPresent();
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isType() {
    return getRole().filter(role -> role == Role.TYPE).isPresent();
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isSpatial() {
    return getType() == Type.GEOMETRY;
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isTemporal() {
    return getType() == Type.DATETIME;
  }

  // TODO
  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isReference() {
    return false;
  }

  // TODO
  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isReferenceEmbed() {
    return false;
  }

  // TODO
  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isForceReversePolygon() {
    return false;
  }
}
