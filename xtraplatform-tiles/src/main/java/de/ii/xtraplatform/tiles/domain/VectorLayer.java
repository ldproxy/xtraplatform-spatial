/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true, deepImmutablesDetection = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(as = ImmutableVectorLayer.class)
@JsonPropertyOrder({"id", "fields", "description", "maxzoom", "minzoom"})
public interface VectorLayer {

  @JsonProperty("id")
  String getId();

  @JsonProperty("fields")
  Map<String, String> getFields();

  @JsonProperty("description")
  Optional<String> getDescription();

  @JsonProperty("geometry_type")
  Optional<String> getGeometryType();

  @JsonProperty("maxzoom")
  Optional<Number> getMaxzoom();

  @JsonProperty("minzoom")
  Optional<Number> getMinzoom();

  @JsonAnyGetter
  Map<String, Object> getAdditionalProperties();

  static VectorLayer of(FeatureSchema featureSchema, Optional<MinMax> minMax) {
    String geometryType =
        VectorLayer.getGeometryTypeAsString(featureSchema.getEffectiveGeometryType());

    Map<String, String> properties =
        featureSchema.getProperties().stream()
            .filter(prop -> !prop.isSpatial())
            .map(
                prop ->
                    new SimpleEntry<>(prop.getName(), VectorLayer.getTypeAsString(prop.getType())))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue, (a, b) -> a));

    return ImmutableVectorLayer.builder()
        .id(featureSchema.getName())
        .description(featureSchema.getDescription().orElse(""))
        .fields(properties)
        .geometryType(geometryType)
        .minzoom(minMax.map(MinMax::getMin))
        .maxzoom(minMax.map(MinMax::getMax))
        .build();
  }

  @JsonIgnore
  @Value.Lazy
  default FeatureSchema toFeatureSchema() {
    ImmutableFeatureSchema geometry =
        new Builder()
            .name("geometry")
            .type(Type.GEOMETRY)
            .geometryType(getGeometryTypeFromString(getGeometryType().orElse("")))
            .build();

    Map<String, FeatureSchema> properties =
        getFields().entrySet().stream()
            .map(
                (entry) -> {
                  ImmutableFeatureSchema property =
                      new Builder()
                          .name(entry.getKey())
                          .type(getTypeFromString(entry.getValue()))
                          .build();
                  return new SimpleEntry<>(entry.getKey(), property);
                })
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    return new ImmutableFeatureSchema.Builder()
        .name(getId())
        .type(Type.OBJECT)
        .description(getDescription())
        .putPropertyMap(geometry.getName(), geometry)
        .putAllPropertyMap(properties)
        .build();
  }

  static String getTypeAsString(SchemaBase.Type type) {
    return switch (type) {
      case INTEGER -> "Integer";
      case FLOAT -> "Number";
      case BOOLEAN -> "Boolean";
      default -> "String";
    };
  }

  static SchemaBase.Type getTypeFromString(String type) {
    return switch (type) {
      case "Number" -> Type.FLOAT;
      case "Boolean" -> Type.BOOLEAN;
      default -> Type.STRING;
    };
  }

  static String getGeometryTypeAsString(GeometryType geometryType) {
    return switch (geometryType) {
      case POINT, MULTI_POINT -> "points";
      case LINE_STRING, MULTI_LINE_STRING -> "lines";
      case POLYGON, MULTI_POLYGON -> "polygons";
      default -> "unknown";
    };
  }

  static GeometryType getGeometryTypeFromString(String geometryType) {
    return switch (geometryType) {
      case "points" -> GeometryType.MULTI_POINT;
      case "lines" -> GeometryType.MULTI_LINE_STRING;
      case "polygons" -> GeometryType.MULTI_POLYGON;
      default -> GeometryType.ANY;
    };
  }
}
