/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.features.domain.FeaturePathTracker;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ModifiableCollectionMetadata;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.immutables.value.Value.Modifiable;

public interface FeatureEventHandlerSimple<T, U, V extends ModifiableContext<T, U>> {

  interface Context<T, U> {

    ModifiableCollectionMetadata metadata();

    List<String> path();

    String pathAsString();

    Optional<SimpleFeatureGeometry> geometryType();

    OptionalInt geometryDimension();

    @Nullable
    String value();

    @Nullable
    Type valueType();

    @Value.Default
    default boolean inGeometry() {
      return false;
    }

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
  }

  interface ModifiableContext<T, U> extends Context<T, U> {

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
      Optional<String> pathSeparator = Optional.empty();
      // Optional.ofNullable(mapping()).flatMap(u -> u.getPathSeparator());

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

    ModifiableContext<T, U> setMetadata(ModifiableCollectionMetadata collectionMetadata);

    ModifiableContext<T, U> setPathTracker(FeaturePathTracker pathTracker);

    ModifiableContext<T, U> setGeometryType(SimpleFeatureGeometry geometryType);

    ModifiableContext<T, U> setGeometryType(Optional<SimpleFeatureGeometry> geometryType);

    ModifiableContext<T, U> setGeometryDimension(int geometryDimension);

    ModifiableContext<T, U> setGeometryDimension(OptionalInt geometryDimension);

    ModifiableContext<T, U> setValue(String value);

    ModifiableContext<T, U> setValueType(Type valueType);

    ModifiableContext<T, U> setInGeometry(boolean inGeometry);

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

  @Modifiable
  @Value.Style(deepImmutablesDetection = true)
  interface GenericContextSimple
      extends FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping> {}

  // T createContext();

  void onStart(V context);

  void onEnd(V context);

  void onFeatureStart(V context);

  void onFeatureEnd(V context);

  void onObjectStart(V context);

  void onObjectEnd(V context);

  void onArrayStart(V context);

  void onArrayEnd(V context);

  void onValue(V context);
}
