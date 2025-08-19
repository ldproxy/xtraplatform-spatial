/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.PropertyBase.Type;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class FeatureObjectTransformerBase<
        T extends SchemaBase<T>,
        U extends SchemaMappingBase<T>,
        V extends PropertyBase<V, T>,
        W extends FeatureBase<V, T>>
    extends FeatureTokenTransformerBase<T, U, ModifiableContext<T, U>> {

  private final Optional<String> nullValue;
  private W currentFeature;
  private V currentObjectOrArray;

  protected FeatureObjectTransformerBase() {
    this(Optional.empty());
  }

  protected FeatureObjectTransformerBase(Optional<String> nullValue) {
    this.nullValue = nullValue;
  }

  public abstract W createFeature();

  public abstract V createProperty();

  public abstract void onFeature(W feature);

  @Override
  public void onStart(ModifiableContext<T, U> context) {}

  @Override
  public void onEnd(ModifiableContext<T, U> context) {}

  @Override
  public final void onFeatureStart(ModifiableContext<T, U> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    this.currentFeature = createFeature();
    currentFeature.schema(context.schema().get());

    currentFeature.collectionMetadata(context.metadata());

    this.currentObjectOrArray = null;
  }

  @Override
  public final void onFeatureEnd(ModifiableContext<T, U> context) {
    onFeature(currentFeature);

    this.currentFeature = null;
    this.currentObjectOrArray = null;
  }

  // TODO: if no matching name is found in schema, when inGeometry use primary
  @Override
  public final void onObjectStart(ModifiableContext<T, U> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    this.currentObjectOrArray =
        createProperty(PropertyBase.Type.OBJECT, context.path(), context.schema().get());
  }

  @Override
  public final void onObjectEnd(ModifiableContext<T, U> context) {
    this.currentObjectOrArray = getCurrentParent();
  }

  @Override
  public final void onArrayStart(ModifiableContext<T, U> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    this.currentObjectOrArray =
        createProperty(PropertyBase.Type.ARRAY, context.path(), context.schema().get());
  }

  @Override
  public final void onArrayEnd(ModifiableContext<T, U> context) {
    this.currentObjectOrArray = getCurrentParent();
  }

  @Override
  public final void onGeometry(ModifiableContext<T, U> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    createProperty(Type.GEOMETRY, context.path(), context.schema().get(), context.geometry());
  }

  @Override
  public final void onValue(ModifiableContext<T, U> context) {
    if (context.schema().isEmpty()
        || context.schema().get().isFeature()
        || Objects.isNull(context.value())) {
      if (nullValue.isPresent() && Objects.equals(context.value(), nullValue.get())) {
        Map<List<String>, T> childMappings = getChildMappings(context.mapping(), context.path());

        childMappings.forEach(
            (path, schema) ->
                createProperty(Type.VALUE, path, schema, nullValue.get(), context.transformed()));
      }
      return;
    }

    createProperty(
        PropertyBase.Type.VALUE,
        context.path(),
        context.schema().get(),
        context.value(),
        context.transformed());
  }

  private V createProperty(Property.Type type, List<String> path, T schema) {
    return createProperty(type, path, schema, null, null, ImmutableMap.of());
  }

  private V createProperty(Property.Type type, List<String> path, T schema, Geometry<?> geometry) {
    return createProperty(type, path, schema, null, geometry, ImmutableMap.of());
  }

  private V createProperty(
      Type type, List<String> path, T schema, String value, Map<String, String> transformed) {
    return createProperty(type, path, schema, value, null, transformed);
  }

  private V createProperty(
      Type type,
      List<String> path,
      T schema,
      String value,
      Geometry<?> geometry,
      Map<String, String> transformed) {

    V property = createProperty();
    property
        .type(type)
        .schema(schema)
        .propertyPath(path)
        .value(value)
        .geometry(geometry)
        .transformed(transformed);

    if (Objects.nonNull(currentObjectOrArray)) {
      property.parent(currentObjectOrArray);
      currentObjectOrArray.addNestedProperties(property);
    } else {
      currentFeature.addProperties(property);
    }

    return property;
  }

  private V getCurrentParent() {
    return Objects.nonNull(currentObjectOrArray) && currentObjectOrArray.getParent().isPresent()
        ? currentObjectOrArray.getParent().get()
        : null;
  }

  private Map<List<String>, T> getChildMappings(U mapping, List<String> path) {
    if (path.isEmpty()) {
      return mapping.getSchemasByTargetPath().entrySet().stream()
          .filter(e -> e.getKey().size() > path.size() && !e.getValue().get(0).isPrimaryGeometry())
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
    }

    return mapping.getSchemasByTargetPath().entrySet().stream()
        .filter(
            e ->
                e.getKey().size() >= path.size()
                    && Objects.equals(path, e.getKey().subList(0, path.size())))
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
  }
}
