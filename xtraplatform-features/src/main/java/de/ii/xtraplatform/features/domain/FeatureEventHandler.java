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

@SuppressWarnings("PMD.TooManyMethods")
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

    /**
     * Captured links for the current feature, populated by {@link
     * FeatureTokenTransformerPropertyLinks} from schema properties with an {@link
     * FeatureSchema#getEffectiveLink() effective link}. Encoders use it to emit per-feature link
     * entries.
     */
    List<PropertyLink> propertyLinks();

    /**
     * Canonical (untransformed) feature id, captured when a profile rewrites the id token (e.g.
     * {@code versions-as-features-unique-ids} produces a composite {@code id.<timestamp>}).
     * Encoders that need the stable id rather than the composite — most notably {@code
     * gml:identifier} — read this; when null, {@code context.value()} on the id property is the
     * canonical id and should be used.
     */
    @Nullable
    String canonicalFeatureId();

    @Value.Lazy
    default Optional<T> schema() {
      if (Objects.isNull(mapping())) {
        return Optional.empty();
      }

      if (isPathEmpty()) {
        return Optional.ofNullable(mapping().getTargetSchema());
      }

      List<T> targetSchemas = schemasForPath();

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

      if (isPathEmpty()) {
        return -1;
      }

      List<Integer> positions = positionsForPath();

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

      if (isPathEmpty()) {
        return List.of();
      }

      // TODO: by target path?
      List<List<Integer>> positions = parentPositionsForPath();

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

      if (isPathEmpty()) {
        return ImmutableList.of();
      }

      List<List<T>> parentSchemas = parentSchemasForPath();

      if (parentSchemas.isEmpty()) {
        return ImmutableList.of();
      }

      int schemaIndex = schemaIndex() > -1 ? schemaIndex() : parentSchemas.size() - 1;
      return parentSchemas.get(schemaIndex);
    }

    // The lookups below back schema()/pos()/parentPos()/parentSchemas(). They are factored into
    // their own methods so ModifiableContext can memoize them per tracked path: schema() etc. are
    // @Value.Lazy, but @Value.Lazy is not cached on a @Value.Modifiable, so without memoization the
    // same path is looked up again on every call. The defaults here are the unmemoized fallback.

    default boolean isPathEmpty() {
      return path().isEmpty();
    }

    default List<T> schemasForPath() {
      return isUseTargetPaths()
          ? mapping().getSchemasForTargetPath(path())
          : mapping().getSchemasForSourcePath(path());
    }

    default List<Integer> positionsForPath() {
      return isUseTargetPaths()
          ? mapping().getPositionsForTargetPath(path())
          : mapping().getPositionsForSourcePath(path());
    }

    default List<List<Integer>> parentPositionsForPath() {
      return isUseTargetPaths()
          ? mapping().getParentPositionsForTargetPath(path())
          : mapping().getParentPositionsForSourcePath(path());
    }

    default List<List<T>> parentSchemasForPath() {
      return isUseTargetPaths()
          ? mapping().getParentSchemasForTargetPath(path())
          : mapping().getParentSchemasForSourcePath(path());
    }

    @Value.Lazy
    default boolean isRequired() {
      return schema().filter(T::isRequired).isPresent();
    }
  }

  interface ModifiableContext<T extends SchemaBase<T>, U extends SchemaMappingBase<T>>
      extends Context<T, U> {

    // a @Value.Default is not cached on a Modifiable, so create the value, store it via the
    // setter and reuse that instance on subsequent calls
    @Override
    @Value.Default
    default ModifiableCollectionMetadata metadata() {
      ModifiableCollectionMetadata collectionMetadata = ModifiableCollectionMetadata.create();

      setMetadata(collectionMetadata);

      return collectionMetadata;
    }

    // a @Value.Default is not cached on a Modifiable, so create the value, store it via the
    // setter and reuse that instance on subsequent calls
    @Value.Default
    default FeaturePathTracker pathTracker() {
      // when tracking target paths, if present, use path separator from flatten transformation in
      // mapping().getTargetSchema()
      Optional<String> pathSeparator =
          Optional.ofNullable(mapping()).flatMap(SchemaMappingBase::getPathSeparator);

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

    // a @Value.Default is not cached on a Modifiable, so create the value, store it via the
    // setter and reuse that instance on subsequent calls
    @Value.Default
    @Value.Auxiliary
    default PathMemo<T> pathMemo() {
      PathMemo<T> pathMemo = new PathMemo<>();

      setPathMemo(pathMemo);

      return pathMemo;
    }

    // Returns the path memo, resetting its cached lookups when the tracked path (or the
    // target/source path mode) has changed since they were last computed.
    private PathMemo<T> currentMemo() {
      PathMemo<T> memo = pathMemo();
      long version = pathTracker().version();
      boolean useTargetPaths = isUseTargetPaths();

      if (memo.version != version || memo.useTargetPaths != useTargetPaths) {
        PathMemo<T> refreshed = new PathMemo<>();
        refreshed.version = version;
        refreshed.useTargetPaths = useTargetPaths;
        refreshed.path = pathTracker().asList();
        setPathMemo(refreshed);
        memo = refreshed;
      }

      return memo;
    }

    @Override
    default boolean isPathEmpty() {
      return pathTracker().isEmpty();
    }

    @Override
    default List<T> schemasForPath() {
      PathMemo<T> memo = currentMemo();
      if (memo.schemas == null) {
        memo.schemas =
            memo.useTargetPaths
                ? mapping().getSchemasForTargetPath(memo.path)
                : mapping().getSchemasForSourcePath(memo.path);
      }
      return memo.schemas;
    }

    @Override
    default List<Integer> positionsForPath() {
      PathMemo<T> memo = currentMemo();
      if (memo.positions == null) {
        memo.positions =
            memo.useTargetPaths
                ? mapping().getPositionsForTargetPath(memo.path)
                : mapping().getPositionsForSourcePath(memo.path);
      }
      return memo.positions;
    }

    @Override
    default List<List<Integer>> parentPositionsForPath() {
      PathMemo<T> memo = currentMemo();
      if (memo.parentPositions == null) {
        memo.parentPositions =
            memo.useTargetPaths
                ? mapping().getParentPositionsForTargetPath(memo.path)
                : mapping().getParentPositionsForSourcePath(memo.path);
      }
      return memo.parentPositions;
    }

    @Override
    default List<List<T>> parentSchemasForPath() {
      PathMemo<T> memo = currentMemo();
      if (memo.parentSchemas == null) {
        memo.parentSchemas =
            memo.useTargetPaths
                ? mapping().getParentSchemasForTargetPath(memo.path)
                : mapping().getParentSchemasForSourcePath(memo.path);
      }
      return memo.parentSchemas;
    }

    @Value.Lazy
    default boolean shouldSkip() {
      return schema().isEmpty() || !shouldInclude(schema().get(), pathTracker().toString());
    }

    private boolean shouldInclude(T schema, String path) {
      return schema.isId()
          || (schema.isSpatial()
              && (typeQueries().isEmpty()
                  || typeQueries().stream().anyMatch(typeQuery -> !typeQuery.skipGeometry())))
          // TODO: enable if projected output needs to be schema valid
          // || isRequired(schema, parentSchemas)
          || (!schema.isId() && propertyIsInFields(path));
    }

    // multiple queries of a multi-query may use the same feature type, the projections of such
    // queries are merged
    private List<? extends TypeQuery> typeQueries() {
      return query() instanceof FeatureQuery
          ? List.of((FeatureQuery) query())
          : query() instanceof MultiFeatureQuery
              ? ((MultiFeatureQuery) query())
                  .getQueries().stream()
                      .filter(subQuery -> Objects.equals(subQuery.getType(), type()))
                      .toList()
              : List.of();
    }

    default boolean propertyIsInFields(String property) {
      List<? extends TypeQuery> typeQueries = typeQueries();
      return !typeQueries.isEmpty()
          && typeQueries.stream()
              .anyMatch(
                  typeQuery ->
                      typeQuery.getFields().isEmpty()
                          || typeQuery.getFields().contains("*")
                          || typeQuery.getFields().stream()
                              .anyMatch(field -> field.startsWith(property)));
    }

    default boolean isRequired(T schema, List<T> parentSchemas) {
      return schema.isRequired()
          && (parentSchemas.size() <= 1
              || parentSchemas.stream().limit(parentSchemas.size() - 1).allMatch(T::isRequired));
    }

    ModifiableContext<T, U> setMetadata(ModifiableCollectionMetadata collectionMetadata);

    ModifiableContext<T, U> setPathTracker(FeaturePathTracker pathTracker);

    ModifiableContext<T, U> setPathMemo(PathMemo<T> pathMemo);

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

    ModifiableContext<T, U> setPropertyLinks(Iterable<? extends PropertyLink> propertyLinks);

    ModifiableContext<T, U> setCanonicalFeatureId(@Nullable String canonicalFeatureId);
  }

  /**
   * Per-context cache for the path-keyed lookups behind {@link Context#schema()}, {@link
   * Context#pos()}, {@link Context#parentPos()} and {@link Context#parentSchemas()}. The lookups
   * are recomputed whenever {@link #version} (the {@link FeaturePathTracker} version) or {@link
   * #useTargetPaths} no longer matches; otherwise the cached values are reused. Mutable, single-
   * threaded scratch state owned by one context instance.
   */
  final class PathMemo<T extends SchemaBase<T>> {
    long version = Long.MIN_VALUE;
    boolean useTargetPaths;
    List<String> path;
    List<T> schemas;
    List<Integer> positions;
    List<List<T>> parentSchemas;
    List<List<Integer>> parentPositions;
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
