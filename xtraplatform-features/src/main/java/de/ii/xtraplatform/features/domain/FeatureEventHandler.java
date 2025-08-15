/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

public interface FeatureEventHandler<
    T extends SchemaBase<T>, U extends SchemaMappingBase<T>, V extends ModifiableContext<T, U>> {

  interface Context<T extends SchemaBase<T>, U extends SchemaMappingBase<T>> {

    ModifiableCollectionMetadata metadata();

    List<String> path();

    String pathAsString();

    @Nullable
    String value();

    @Nullable
    Type valueType();

    @Nullable
    Geometry<?> geometry();

    @Value.Default
    default boolean inObject() {
      return false;
    }

    @Value.Default
    default boolean inArray() {
      return false;
    }

    List<Integer> indexes();

    @Value.Lazy
    default long index() {
      return indexes().isEmpty() ? 0 : indexes().get(indexes().size() - 1);
    }

    Query query();

    @Nullable
    String type();

    Map<String, U> mappings();

    @Nullable
    @Value.Lazy
    default U mapping() {
      return Objects.isNull(type()) ? null : mappings().get(type());
    }

    @Value.Default
    default int schemaIndex() {
      return -1;
    }

    Map<String, String> transformed();

    @Value.Default
    default boolean isUseTargetPaths() {
      return false;
    }

    Map<String, String> additionalInfo();

    @Value.Lazy
    default Optional<T> schema() {
      if (Objects.isNull(mapping())) {
        return Optional.empty();
      }

      List<String> path = path();

      if (path.isEmpty()) {
        return Optional.ofNullable(mapping().getTargetSchema());
      }

      List<T> targetSchemas =
          isUseTargetPaths()
              ? mapping().getSchemasForTargetPath(path)
              : mapping().getSchemasForSourcePath(path);

      if (targetSchemas.isEmpty()) {
        // No mapping found for path
        return Optional.empty();
      }

      int schemaIndex = schemaIndex() > -1 ? schemaIndex() : targetSchemas.size() - 1;
      T targetSchema = targetSchemas.size() > schemaIndex ? targetSchemas.get(schemaIndex) : null;

      return Optional.ofNullable(targetSchema);
    }

    @Value.Lazy
    default int pos() {
      if (Objects.isNull(mapping())) {
        return -1;
      }

      List<String> path = path();

      if (path.isEmpty()) {
        return -1;
      }

      List<Integer> positions =
          isUseTargetPaths()
              ? mapping().getPositionsForTargetPath(path)
              : mapping().getPositionsForSourcePath(path);

      int schemaIndex = schemaIndex() > -1 ? schemaIndex() : positions.size() - 1;
      if (positions.size() > schemaIndex) {
        return positions.get(schemaIndex);
      }

      return -1;
    }

    @Value.Lazy
    default List<Integer> parentPos() {
      if (Objects.isNull(mapping())) {
        return List.of();
      }

      List<String> path = path();

      if (path.isEmpty()) {
        return List.of();
      }

      // TODO: by target path?
      List<List<Integer>> positions =
          isUseTargetPaths()
              ? mapping().getParentPositionsForTargetPath(path)
              : mapping().getParentPositionsForSourcePath(path);

      int schemaIndex = schemaIndex() > -1 ? schemaIndex() : positions.size() - 1;
      if (positions.size() > schemaIndex) {
        return positions.get(schemaIndex);
      }

      return List.of();
    }

    @Value.Lazy
    default List<T> parentSchemas() {
      if (Objects.isNull(mapping())) {
        return ImmutableList.of();
      }

      List<String> path = path();

      if (path.isEmpty()) {
        return ImmutableList.of();
      }

      List<List<T>> parentSchemas =
          isUseTargetPaths()
              ? mapping().getParentSchemasForTargetPath(path)
              : mapping().getParentSchemasForSourcePath(path);

      if (parentSchemas.isEmpty()) {
        return ImmutableList.of();
      }

      int schemaIndex = schemaIndex() > -1 ? schemaIndex() : parentSchemas.size() - 1;
      return parentSchemas.get(schemaIndex);
    }

    @Value.Lazy
    default boolean isRequired() {
      return schema().filter(T::isRequired).isPresent();
    }
  }

  interface ModifiableContext<T extends SchemaBase<T>, U extends SchemaMappingBase<T>>
      extends Context<T, U> {

    // TODO: default values are not cached by Modifiable
    @Value.Default
    default ModifiableCollectionMetadata metadata() {
      ModifiableCollectionMetadata collectionMetadata = ModifiableCollectionMetadata.create();

      setMetadata(collectionMetadata);

      return collectionMetadata;
    }

    // TODO: default values are not cached by Modifiable
    @Value.Default
    default FeaturePathTracker pathTracker() {
      // when tracking target paths, if present, use path separator from flatten transformation in
      // mapping().getTargetSchema()
      Optional<String> pathSeparator =
          Optional.ofNullable(mapping()).flatMap(u -> u.getPathSeparator());

      FeaturePathTracker pathTracker =
          pathSeparator.isPresent()
              ? new FeaturePathTracker(pathSeparator.get())
              : new FeaturePathTracker();

      setPathTracker(pathTracker);

      return pathTracker;
    }

    @Value.Lazy
    @Override
    default List<String> path() {
      return pathTracker().asList();
    }

    @Value.Lazy
    @Override
    default String pathAsString() {
      return pathTracker().toStringWithDefaultSeparator();
    }

    @Value.Lazy
    default boolean shouldSkip() {
      return schema().isEmpty()
          || !shouldInclude(schema().get(), parentSchemas(), pathTracker().toString());
    }

    private boolean shouldInclude(T schema, List<T> parentSchemas, String path) {
      return schema.isId()
          || (schema.isSpatial() && (Objects.isNull(typeQuery()) || !typeQuery().skipGeometry()))
          // TODO: enable if projected output needs to be schema valid
          // || isRequired(schema, parentSchemas)
          || (!schema.isId() && propertyIsInFields(path));
    }

    private TypeQuery typeQuery() {
      return query() instanceof FeatureQuery
          ? (FeatureQuery) query()
          : query() instanceof MultiFeatureQuery
              ? ((MultiFeatureQuery) query())
                  .getQueries().stream()
                      .filter(subQuery -> Objects.equals(subQuery.getType(), type()))
                      .findFirst()
                      .orElse(null)
              : null;
    }

    default boolean propertyIsInFields(String property) {
      TypeQuery typeQuery = typeQuery();
      return Objects.nonNull(typeQuery)
          && (typeQuery.getFields().isEmpty()
              || typeQuery.getFields().contains("*")
              || typeQuery.getFields().stream().anyMatch(field -> field.startsWith(property)));
    }

    default boolean isRequired(T schema, List<T> parentSchemas) {
      return schema.isRequired()
          && (parentSchemas.size() <= 1
              || parentSchemas.stream().limit(parentSchemas.size() - 1).allMatch(T::isRequired));
    }

    ModifiableContext<T, U> setMetadata(ModifiableCollectionMetadata collectionMetadata);

    ModifiableContext<T, U> setPathTracker(FeaturePathTracker pathTracker);

    ModifiableContext<T, U> setValue(String value);

    ModifiableContext<T, U> setValueType(SchemaBase.Type valueType);

    ModifiableContext<T, U> setGeometry(Geometry<?> geometry);

    ModifiableContext<T, U> setInObject(boolean inObject);

    ModifiableContext<T, U> setInArray(boolean inArray);

    ModifiableContext<T, U> setIndexes(Iterable<Integer> indexes);

    ModifiableContext<T, U> setQuery(Query query);

    ModifiableContext<T, U> setType(String type);

    ModifiableContext<T, U> setMappings(Map<String, ? extends U> mappings);

    ModifiableContext<T, U> setSchemaIndex(int schemaIndex);

    ModifiableContext<T, U> setTransformed(Map<String, ? extends String> transformed);

    ModifiableContext<T, U> setIsUseTargetPaths(boolean isUseTargetPaths);

    ModifiableContext<T, U> putAdditionalInfo(String key, String value);
  }

  // T createContext();

  void onStart(V context);

  void onEnd(V context);

  void onFeatureStart(V context);

  void onFeatureEnd(V context);

  void onObjectStart(V context);

  void onObjectEnd(V context);

  void onArrayStart(V context);

  void onArrayEnd(V context);

  void onGeometry(V context);

  void onValue(V context);
}
