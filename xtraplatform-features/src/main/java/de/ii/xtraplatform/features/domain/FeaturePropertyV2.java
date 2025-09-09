/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.entities.domain.maptobuilder.encoding.BuildableMapEncodingEnabled;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
@BuildableMapEncodingEnabled
@JsonDeserialize(builder = ImmutableFeaturePropertyV2.Builder.class)
public interface FeaturePropertyV2 extends Buildable<FeaturePropertyV2> {

  enum Role {
    ID
  }

  enum Type {
    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    DATETIME,
    GEOMETRY,
    OBJECT,
    VALUE_ARRAY,
    OBJECT_ARRAY,
    UNKNOWN
  }

  @JsonIgnore
  String getName();

  String getPath();

  @Value.Default
  default Type getType() {
    return Type.STRING;
  }

  Optional<String> getObjectType();

  Optional<Type> getValueType();

  Optional<Role> getRole();

  Optional<String> getLabel();

  Optional<String> getDescription();

  Optional<GeometryType> getGeometryType();

  Map<String, String> getTransformers();

  Optional<Constraints> getConstraints();

  // behaves exactly like Map<String, FeaturePropertyV2>, but supports mergeable builder
  // deserialization
  // (immutables attributeBuilder does not work with maps yet)
  @JsonMerge
  BuildableMap<FeaturePropertyV2, ImmutableFeaturePropertyV2.Builder> getProperties();

  Map<String, String> getAdditionalInfo();

  // custom builder to automatically use keys of types as name of nested FeaturePropertyV2
  abstract class Builder implements BuildableBuilder<FeaturePropertyV2> {

    public abstract ImmutableFeaturePropertyV2.Builder putProperties(
        String key, ImmutableFeaturePropertyV2.Builder builder);

    @JsonProperty(value = "properties")
    public ImmutableFeaturePropertyV2.Builder putProperties2(
        String key, ImmutableFeaturePropertyV2.Builder builder) {
      return putProperties(key, builder.name(key));
    }
  }

  @Value.Immutable
  @Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
  @JsonDeserialize(builder = ImmutableConstraints.Builder.class)
  interface Constraints {

    Optional<String> getCodelist();

    @JsonProperty(value = "enum")
    List<String> getEnumValues();

    Optional<String> getRegex();

    Optional<Boolean> getRequired();

    Optional<Double> getMin();

    Optional<Double> getMax();

    Optional<Integer> getMinOccurrence();

    Optional<Integer> getMaxOccurrence();
  }

  @Override
  default ImmutableFeaturePropertyV2.Builder getBuilder() {
    return new ImmutableFeaturePropertyV2.Builder().from(this);
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isId() {
    return getRole().filter(role -> role == Role.ID).isPresent();
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

  @Value.Check
  default void check() {
    Type type = getType();
    if (!getProperties().isEmpty()) {
      Preconditions.checkState(
          type == Type.OBJECT || type == Type.OBJECT_ARRAY,
          "Properties should only be filled when Type is OBJECT or OBJECT_ARRAY. Current type: %s",
          type);
    }
    if (getObjectType().isPresent()) {
      Preconditions.checkState(
          type == Type.OBJECT || type == Type.OBJECT_ARRAY,
          "ObjectType should only be set when Type is OBJECT or OBJECT_ARRAY. Current type: %s",
          type);
    }
    if (getValueType().isPresent()) {
      Preconditions.checkState(
          type == Type.VALUE_ARRAY,
          "ValueType should only be set when Type is VALUE_ARRAY. Current type: %s",
          type);
    }
    if (type == Type.VALUE_ARRAY) {
      Preconditions.checkState(
          getValueType().isPresent(), "ValueType must be set when Type is VALUE_ARRAY");
    }
    if (getGeometryType().isPresent()) {
      Preconditions.checkState(
          type == Type.GEOMETRY,
          "GeometryType should only be set when Type is Geometry, current type: %s",
          getType());
    }
    if (type == Type.GEOMETRY) {
      Preconditions.checkState(
          getGeometryType().isPresent(), "GeometryType must be set when Type is Geometry,");
    }
  }
}
