/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.transform.FeaturePropertyTransformerFlatten;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

public interface PropertyBase<T extends PropertyBase<T, U>, U extends SchemaBase<U>> {

  enum Type {
    VALUE,
    ARRAY,
    OBJECT,
    GEOMETRY
  }

  Type getType();

  Optional<U> getSchema();

  @Value.Default
  default String getName() {
    return getSchema().map(U::getName).orElse("");
  }

  @Nullable
  String getValue();

  @Nullable
  Geometry<?> getGeometry();

  @Value.Auxiliary
  Optional<T> getParent();

  List<T> getNestedProperties();

  List<String> getPropertyPath();

  @Value.Default
  default int getLevel() {
    return getPropertyPath().size();
  }

  Map<String, String> getTransformed();

  @Value.Derived
  default boolean isValue() {
    return getType() == Type.VALUE;
  }

  @Value.Derived
  default boolean isObject() {
    return getType() == Type.OBJECT;
  }

  @Value.Derived
  default boolean isArray() {
    return getType() == Type.ARRAY;
  }

  @Value.Lazy
  default boolean isFlattened() {
    return getTransformed().containsKey(PropertyTransformations.WILDCARD)
        && getTransformed()
            .get(PropertyTransformations.WILDCARD)
            .contains(FeaturePropertyTransformerFlatten.TYPE);
  }

  Optional<GeometryType> getGeometryType();

  PropertyBase<T, U> schema(Optional<U> schema);

  PropertyBase<T, U> schema(U schema);

  PropertyBase<T, U> name(String name);

  PropertyBase<T, U> type(Type type);

  PropertyBase<T, U> value(String value);

  PropertyBase<T, U> geometry(Geometry<?> geometry);

  PropertyBase<T, U> parent(T parent);

  PropertyBase<T, U> addNestedProperties(T element);

  PropertyBase<T, U> propertyPath(Iterable<String> path);

  PropertyBase<T, U> level(int level);

  PropertyBase<T, U> transformed(Map<String, ? extends String> transformed);

  PropertyBase<T, U> geometryType(Optional<GeometryType> geometryType);

  PropertyBase<T, U> geometryType(GeometryType geometryType);
}
