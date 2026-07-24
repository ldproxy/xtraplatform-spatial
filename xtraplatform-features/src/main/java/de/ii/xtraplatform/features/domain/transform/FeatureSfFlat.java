/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableSortedMap;
import de.ii.xtraplatform.features.domain.FeatureBase;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

@Value.Modifiable
@Value.Style(set = "*")
public interface FeatureSfFlat extends FeatureBase<PropertySfFlat, FeatureSchema> {

  @Value.Lazy
  default Optional<PropertySfFlat> getId() {
    return getProperties().stream()
        .filter(property -> property.getSchema().filter(SchemaBase::isId).isPresent())
        .findFirst();
  }

  @Value.Lazy
  default String getIdValue() {
    return getId().map(PropertySfFlat::getValue).orElse(null);
  }

  @Value.Lazy
  default SortedMap<String, Object> getPropertiesAsMap() {
    return getProperties().stream()
        .map(property -> new SimpleImmutableEntry<>(property.getName(), getValue(property, false)))
        .filter(entry -> Objects.nonNull(entry.getValue()))
        .collect(
            ImmutableSortedMap.toImmutableSortedMap(
                String::compareTo, Map.Entry::getKey, Map.Entry::getValue));
  }

  // Since properties must be "flat" (no arrays or objects), we map any arrays or objects to
  // a string using a JSON representation of arrays and objects.
  @SuppressWarnings("PMD.CyclomaticComplexity")
  private Object getValue(PropertySfFlat property, boolean withQuotes) {
    switch (property.getType()) {
      case VALUE:
        return switch (property.getSchema().map(FeatureSchema::getType).orElse(Type.UNKNOWN)) {
          case BOOLEAN ->
              "t".equalsIgnoreCase(property.getValue())
                  || "true".equalsIgnoreCase(property.getValue())
                  || "1".equals(property.getValue());

          case INTEGER -> {
            try {
              yield Long.parseLong(property.getValue());
            } catch (NumberFormatException e) {
              // ignore
              yield null;
            }
          }

          case FLOAT -> {
            try {
              yield Double.parseDouble(property.getValue());
            } catch (NumberFormatException e) {
              // ignore
              yield null;
            }
          }

          case DATE, DATETIME, STRING, FEATURE_REF, UNKNOWN ->
              withQuotes ? "'" + property.getValue() + "'" : property.getValue();

          case GEOMETRY ->
              // geometries are handled separately, ignore them in this map
              null;

          default -> null;
        };

      case OBJECT:
        return property.getSchema().map(FeatureSchema::getType).orElse(Type.UNKNOWN)
                == Type.GEOMETRY
            ? null
            : getObjectAsString(property);

      case ARRAY:
        return getArrayAsString(property);

      default:
        return null;
    }
  }

  private String getObjectAsString(PropertySfFlat property) {
    String value =
        property.getNestedProperties().stream()
            .map(
                p -> {
                  Object val = getValue(p, true);
                  if (Objects.isNull(val)) {
                    return null;
                  }
                  return String.format("'%s': %s", p.getName(), val);
                })
            .filter(Objects::nonNull)
            .collect(Collectors.joining(", "));
    if (value.isBlank()) {
      return null;
    }
    return String.format("{ %s }", value);
  }

  private String getArrayAsString(PropertySfFlat property) {
    String value =
        property.getNestedProperties().stream()
            .map(
                p -> {
                  Object val = getValue(p, true);
                  if (Objects.isNull(val)) {
                    return null;
                  }
                  return val.toString();
                })
            .filter(Objects::nonNull)
            .collect(Collectors.joining(", "));
    if (value.isBlank()) {
      return null;
    }
    return String.format("[ %s ]", value);
  }

  @Value.Lazy
  default boolean hasGeometry() {
    return getGeometryProperty().isPresent();
  }

  @Value.Lazy
  default Optional<PropertySfFlat> getGeometryProperty() {
    return getProperties().stream()
        .filter(
            property ->
                property
                    .getSchema()
                    .filter(SchemaBase::isSpatial)
                    .filter(SchemaBase::isPrimaryGeometry)
                    .isPresent())
        .findFirst();
  }

  default Optional<Geometry> getJtsGeometry(GeometryFactory geometryFactory) {
    return getGeometryProperty().flatMap(geometry -> geometry.getJtsGeometry(geometryFactory));
  }
}
