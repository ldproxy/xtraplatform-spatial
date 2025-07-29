/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureStoreRelation;
import de.ii.xtraplatform.features.domain.PropertyBase;
import de.ii.xtraplatform.features.domain.PropertyBase.Type;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.WktWkbGeometryType;
import java.io.StringWriter;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

public interface ObjectSql {

  List<FeatureStoreRelation> getPath();

  @Value.Derived
  default Optional<String> getPath2() {
    return this instanceof FeatureSql
        ? ((FeatureSql) this).getSchema().map(SchemaSql::getName)
        : this instanceof PropertySql
            ? ((PropertySql) this).getSchema().map(SchemaSql::getName)
            : Optional.empty();
  }

  Map<String, String> getIds();

  // TODO
  @Value.Derived
  default Map<List<String>, List<Integer>> getRowCounts() {
    return getRowCounts(ImmutableList.of(), 0);
  }

  default Map<List<String>, List<Integer>> getRowCounts(List<String> parentPath, int parentRow) {
    /*
           Map<List<FeatureStoreRelation>, List<Integer>> rows = new LinkedHashMap<>();

           ListMultimap<List<FeatureStoreRelation>, PropertySql> properties = getNested();

           for (List<FeatureStoreRelation> path: properties.keySet()) {
               if (path.size() > 1) {
                   int nestingLevel = path.get(0)
                                          .isM2N() ? 2 : 1;

                   //rows.putIfAbsent(path, new ArrayList<>());

                   List<PropertySql> newParents = properties.get(path.subList(0, 1));
                   for (int i = 0; i < newParents.size(); i++) {
                       Map<List<FeatureStoreRelation>, List<Integer>> nestedRowCounts = newParents.get(i)
                                                                                            .getRowCounts(i);

                       rows.putAll(nestedRowCounts);
                   }

               }

               rows.putIfAbsent(path, IntStream.range(0, parentRow).boxed().collect(Collectors.toList()));
               rows.get(path).add(parentRow, properties.get(path).size());
           }


    */

    Map<List<String>, List<Integer>> rows = new LinkedHashMap<>();

    Map<List<String>, PropertySql> allChildren2 = getAllChildren2();

    allChildren2.forEach(
        (path, property) -> {
          List<Integer> parentRows = ImmutableList.of();

          for (int i = path.size() - 1; i > 0; i--) {
            if (rows.containsKey(path.subList(0, i))) {
              parentRows = rows.get(path.subList(0, i));
              break;
            }
          }

          if (property.getType() == PropertyBase.Type.OBJECT) {
            rows.put(path, ImmutableList.copyOf(Iterables.concat(parentRows, ImmutableList.of(1))));

          } else if (property.getType() == PropertyBase.Type.ARRAY) {
            rows.put(
                path,
                ImmutableList.copyOf(
                    Iterables.concat(parentRows, ImmutableList.of(property.getChildren().size()))));
          }
        });
    /*
    ListMultimap<List<String>, PropertySql> properties = getNested();

    PropertyBase.Type type = this instanceof PropertySql ? ((PropertySql) this).getType() : null;

    if (getPath2().isPresent()) {
        List<String> path = ImmutableList.<String>builder().addAll(parentPath)
                                                           .addAll(Splitter.on('/')
                                                                           .omitEmptyStrings()
                                                                           .splitToList(getPath2().get()))
                                                           .build();

        if (Objects.equals(type, PropertyBase.Type.VALUE)) {
            return rows;
        }

        if (Objects.equals(type, PropertyBase.Type.OBJECT) || Objects.isNull(type)) {
            for (PropertySql propertySql : properties.values()) {

                Map<List<String>, List<Integer>> nestedRowCounts = propertySql.getRowCounts(path, parentRow);

                rows.putAll(nestedRowCounts);
            }
        }

        if (Objects.equals(type, PropertyBase.Type.ARRAY) && !properties.values()
                                                                        .isEmpty()) {

            PropertySql next = properties.values()
                                         .iterator()
                                         .next();
            if (next.getPath2()
                    .isPresent()) {
                List<String> nextPath = ImmutableList.<String>builder().addAll(path)
                                                                       .addAll(Splitter.on('/')
                                                                                       .omitEmptyStrings()
                                                                                       .splitToList(next.getPath2()
                                                                                                        .get()))
                                                                       .build();
                //String nextPath = String.format("%s/%s", path, next.getPath2().get());
                rows.put(nextPath, ImmutableList.of(properties.values()
                                                              .size()));
            }

            //int i = 0;
            //for (PropertySql propertySql : properties.values()) {
            //    Map<String, List<Integer>> rowCounts = propertySql.getRowCounts(path, i);
            //    rows.putAll(rowCounts);
            //    i++;
            //}
        }
    }*/

    return rows;
  }

  ObjectSql path(Iterable<? extends FeatureStoreRelation> path);

  ObjectSql putIds(String key, String value);

  // ObjectSql putRowCounts(List<FeatureStoreRelation> key, List<Integer> value);

  default void putChildrenIds(String name, String id) {
    getAllChildren3().forEach(propertySql -> propertySql.putIds(name, id));
  }

  @Value.Derived
  default Map<String, String> getValues(
      Optional<CrsTransformer> crsTransformer, EpsgCrs nativeCrs) {
    return getChildren().stream()
        .filter(
            propertySql ->
                (propertySql.getType() == PropertyBase.Type.VALUE
                        && Objects.nonNull(propertySql.getValue()))
                    || (propertySql.getType() == Type.OBJECT
                        && propertySql
                            .getSchema()
                            .map(schemaSql -> schemaSql.getType() == SchemaBase.Type.GEOMETRY)
                            .orElse(false)))
        .map(
            propertySql -> {
              boolean isGeometry =
                  propertySql.getType() == Type.OBJECT
                      && propertySql
                          .getSchema()
                          .map(schemaSql -> schemaSql.getType() == SchemaBase.Type.GEOMETRY)
                          .orElse(false);

              if (isGeometry) {
                return toWkt(propertySql, crsTransformer, nativeCrs);
              }

              boolean needsQuotes =
                  propertySql
                      .getSchema()
                      .map(
                          schemaSql ->
                              (schemaSql.getType() == SchemaBase.Type.FEATURE_REF
                                      && schemaSql.getValueType().orElse(SchemaBase.Type.STRING)
                                          == SchemaBase.Type.STRING)
                                  || schemaSql.getType() == SchemaBase.Type.STRING
                                  || schemaSql.getType() == SchemaBase.Type.DATETIME)
                      .orElse(false);

              String value =
                  needsQuotes
                      ? String.format("'%s'", propertySql.getValue().replaceAll("'", "''"))
                      : propertySql.getValue();

              return new AbstractMap.SimpleImmutableEntry<>(propertySql.getName(), value);
            })
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  default Map.Entry<String, String> toWkt(
      PropertySql propertySql, Optional<CrsTransformer> crsTransformer, EpsgCrs nativeCrs) {
    GeometryType geometryType =
        propertySql
            .getGeometryType()
            .or(
                () ->
                    propertySql
                        .getSchema()
                        .flatMap(SchemaSql::getGeometryType)
                        .filter(GeometryType::isSpecific))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Cannot encode geometry as WKT, no specific geometry type found."));
    WktWkbGeometryType wktType = WktWkbGeometryType.fromGeometryType(geometryType);
    Integer dimension = 2;

    StringWriter geometryWriter = new StringWriter();
    /* FIXME
    ImmutableCoordinatesTransformer.Builder coordinatesTransformerBuilder =
        ImmutableCoordinatesTransformer.builder();
    coordinatesTransformerBuilder.coordinatesWriter(
        ImmutableCoordinatesWriterWkt.of(geometryWriter, Optional.ofNullable(dimension).orElse(2)));

    coordinatesTransformerBuilder.crsTransformer(crsTransformer);

    if (dimension != null) {
      coordinatesTransformerBuilder.sourceDimension(dimension);
      coordinatesTransformerBuilder.targetDimension(dimension);
    }

    geometryWriter.append(wktType.toString());

    try {
      toWktArray(propertySql, geometryWriter, coordinatesTransformerBuilder);
    } catch (IOException e) {

    }
     */

    // TODO: functions from Dialect
    return new AbstractMap.SimpleImmutableEntry<>(
        propertySql.getName(),
        String.format(
            "ST_ForcePolygonCW(ST_GeomFromText('%s',%s))", geometryWriter, nativeCrs.getCode()));
  }

  /* FIXME
  // TODO: test all geo types
  default void toWktArray(
      PropertySql propertySql, Writer structureWriter, Builder coordinatesWriterBuilder)
      throws IOException {
    if (propertySql.getType() == PropertyBase.Type.ARRAY) {
      structureWriter.append("(");
    }

    if (propertySql.getType() == PropertyBase.Type.ARRAY
        && !propertySql.getNestedProperties().isEmpty()
        && propertySql.getNestedProperties().get(0).getType() == Type.VALUE) {
      Writer coordinatesWriter = coordinatesWriterBuilder.build();

      for (int i = 0; i < propertySql.getNestedProperties().size(); i++) {
        coordinatesWriter.append(propertySql.getNestedProperties().get(i).getValue());
        if (i < propertySql.getNestedProperties().size() - 1) {
          coordinatesWriter.append(",");
        }
      }

      coordinatesWriter.flush();
    } else {
      for (int i = 0; i < propertySql.getNestedProperties().size(); i++) {
        PropertySql propertySql1 = propertySql.getNestedProperties().get(i);
        if (propertySql1.getType() == PropertyBase.Type.ARRAY) {
          toWktArray(propertySql1, structureWriter, coordinatesWriterBuilder);
          if (i < propertySql.getNestedProperties().size() - 1) {
            structureWriter.append(",");
          }
        }
      }
    }

    if (propertySql.getType() == PropertyBase.Type.ARRAY) {
      structureWriter.append(")");
    }
  }
   */

  default Optional<PropertySql> findChild(List<String> path) {
    if (path.size() < 2) {
      return Optional.empty();
    }

    Map<List<String>, PropertySql> properties = getAllChildren();

    for (int i = 2; i <= path.size(); i++) {
      List<String> subPath = path.subList(0, i);
      if (properties.containsKey(subPath)) {
        return Optional.of(properties.get(subPath));
      }
    }

    return Optional.empty();
  }

  default Optional<ObjectSql> getNestedObject(List<String> path, List<Integer> parentRows) {

    Optional<PropertySql> child = findChild(path);

    if (!child.isPresent()) {
      return Optional.empty();
    }

    PropertySql property = child.get();

    if (property.getType() == PropertyBase.Type.ARRAY) {
      int row = parentRows.isEmpty() ? 0 : parentRows.get(0);

      if (row > property.getNestedProperties().size() - 1) {
        throw new IllegalStateException(
            String.format("No values found for row %s of %s", row, path));
      }

      property = property.getNestedProperties().get(row);
    }

    if (property.getSchema().isPresent()
        && property.getSchema().get().getFullPath().size() < path.size()) {
      return property.getNestedObject(path, parentRows.subList(1, parentRows.size()));
    }

    return Optional.of(property);
  }

  @Value.Derived
  default List<PropertySql> getChildren() {
    return this instanceof FeatureSql
        ? ((FeatureSql) this).getProperties()
        : this instanceof PropertySql
            ? ((PropertySql) this).getNestedProperties()
            : ImmutableList.of();
  }

  @Value.Derived
  default Map<List<String>, PropertySql> getAllChildren() {
    return getChildren().stream()
        .filter(
            propertySql ->
                propertySql.getType() == PropertyBase.Type.OBJECT
                    || propertySql.getType() == PropertyBase.Type.ARRAY)
        .map(
            propertySql ->
                new AbstractMap.SimpleImmutableEntry<>(
                    propertySql.getSchema().map(SchemaBase::getFullPath).orElse(ImmutableList.of()),
                    propertySql))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Value.Derived
  default Map<List<String>, PropertySql> getAllChildren2() {
    return getChildren().stream()
        .filter(
            propertySql ->
                propertySql.getType() == PropertyBase.Type.OBJECT
                    || propertySql.getType() == PropertyBase.Type.ARRAY)
        .filter(
            propertySql ->
                propertySql
                    .getSchema()
                    .map(schemaSql -> schemaSql.getType() != SchemaBase.Type.GEOMETRY)
                    .orElse(true))
        .flatMap(
            propertySql -> {
              List<String> path =
                  propertySql.getSchema().map(SchemaBase::getFullPath).orElse(ImmutableList.of());
              return Stream.concat(
                  Stream.of(new AbstractMap.SimpleImmutableEntry<>(path, propertySql)),
                  propertySql.getAllChildren2().entrySet().stream()
                      .filter(entry -> !Objects.equals(entry.getKey(), path)));
            })
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Value.Derived
  default List<PropertySql> getAllChildren3() {
    return getChildren().stream()
        .filter(
            propertySql ->
                propertySql.getType() == PropertyBase.Type.OBJECT
                    || propertySql.getType() == PropertyBase.Type.ARRAY)
        .flatMap(
            propertySql ->
                Stream.concat(Stream.of(propertySql), propertySql.getAllChildren3().stream()))
        .collect(ImmutableList.toImmutableList());
  }

  @Value.Derived
  default ListMultimap<List<String>, PropertySql> getNested() {
    return getChildren().stream()
        // .filter(propertySql -> propertySql.getPath2().isPresent())
        .map(
            propertySql ->
                new AbstractMap.SimpleImmutableEntry<>(
                    propertySql.getSchema().map(SchemaBase::getFullPath).orElse(ImmutableList.of()),
                    propertySql))
        .collect(
            ImmutableListMultimap.toImmutableListMultimap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
