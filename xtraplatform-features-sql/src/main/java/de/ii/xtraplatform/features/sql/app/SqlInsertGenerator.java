/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

/**
 * @author zahnen
 */
class SqlInsertGenerator {

  /*private static final Logger LOGGER = LoggerFactory.getLogger(SqlInsertGenerator.class);

  //NOTE: replace attributesContainer + relationPath with SourceSchemaSql
  // attributesContainer:
  // - columns, sortKey -> derive in schema
  // - mainContainer id -> set in schema
  // -
  // relationPath:
  // - parentRelation is still available
  // - parentPath and mainPath are used to get ids; couldn't these (as well as columns) already be part of the schema?
  public Function<FeatureSql, Pair<String, Optional<Consumer<String>>>> createInsert(
          FeatureStoreAttributesContainer attributesContainer, List<FeatureStoreRelation> relationPath,
          List<Integer> parentRows, boolean withId) {

      Optional<FeatureStoreRelation> parentRelation = !relationPath.isEmpty() ? Optional.ofNullable(relationPath.get(relationPath.size() - 1)) : Optional.empty();

      List<String> columns3 = attributesContainer.getAttributes()
                                                 .stream()
                                                 .map(FeatureStoreAttribute::getName)
                                                 .filter(col -> withId || !col.equals(attributesContainer.getSortKey()))
                                                 .collect(Collectors.toList());

      //NOTE: from Syntax
      List<String> columns2 = columns3.stream()
                                      .map(col -> col.startsWith("ST_AsText(ST_ForcePolygonCCW(") ? col.substring("ST_AsText(ST_ForcePolygonCCW(".length(), col.length() - 2) : col)
                                      .collect(Collectors.toList());

      List<String> idKeys = new ArrayList<>();

      Optional<FeatureStoreAttributesContainer> mainContainer = !attributesContainer.isMain() && attributesContainer instanceof FeatureStoreInstanceContainer
              ? ((FeatureStoreInstanceContainer) attributesContainer).getMainAttributesContainer()
              : Optional.empty();

      if (!attributesContainer.isMain() && mainContainer.isPresent()) {
          FeatureStoreAttribute idAttribute = mainContainer.get()
                                                           .getIdAttribute()
                                                           .get();

          idKeys.add(0, mainContainer.get()
                                     .getName() + "." + idAttribute.getName());
          columns2.add(0, attributesContainer.getSortKey());
      }

      if (parentRelation.isPresent()) {
          //NOTE: is this merged?
          if (!attributesContainer.isMain() && parentRelation.get()
                                                             .isOne2One() && Objects.equals(parentRelation.get()
                                                                                                          .getSourceSortKey(), parentRelation.get()
                                                                                                                                             .getSourceField())) {
              // NOTE: fullPath, sortKey
              idKeys.add(0, attributesContainer.getInstanceContainerName() + ".id");
              columns2.add(0, "id");
          } else if (parentRelation.get()
                                   .isOne2N()) {
              idKeys.add(0, parentRelation.get()
                                          .getSourceContainer() + "." + parentRelation.get()
                                                                                      .getSourceSortKey());
              columns2.add(0, parentRelation.get()
                                            .getSourceSortKey());
          }
      }

      String tableName = attributesContainer.getName();
      String columnNames = Joiner.on(',')
                                 .skipNulls()
                                 .join(columns2);
      if (!columnNames.isEmpty()) {
          columnNames = "(" + columnNames + ")";
      }
      String finalColumnNames = columnNames;

      //NOTE: primaryKey instead of id
      String returningId = parentRelation.isPresent() && parentRelation.get()
                                                                       .isOne2N() ? " RETURNING null" : " RETURNING id";
      Optional<String> returningName = parentRelation.isPresent() && parentRelation.get()
                                                                                   .isOne2N() ? Optional.empty() : Optional.of(tableName + ".id");

      //NOTE: either use List<FeatureStoreRelation> in nestedRow.getNested or extract path formatting
      //List<String> trail = relationPath.isEmpty() ? ImmutableList.of("/" + tableName) : getTrail(relationPath);

      List<FeatureStoreRelation> parentPath = relationPath.size() > 1 ? relationPath.subList(0, relationPath.size() - 1) : ImmutableList.of();
      List<FeatureStoreRelation> mainPath = attributesContainer.isMain() ? relationPath : mainContainer.isPresent() ? ((FeatureStoreRelatedContainer) mainContainer.get()).getInstanceConnection() : ImmutableList.of();


      return feature -> {
          ObjectSql currentRow = relationPath.isEmpty() ? feature : feature.getNestedObject(toPath(relationPath), parentRows);
          Map<String, String> ids = ImmutableMap.<String, String>builder()
                  .putAll(!mainPath.isEmpty() ? feature.getNestedObject(toPath(mainPath), ImmutableList.of())
                                                       .getIds() : ImmutableMap.of())
                  .putAll(parentPath.isEmpty() ? feature.getIds() : feature.getNestedObject(toPath(parentPath), parentRows)
                                                                           .getIds())
                  .build();
          String values = getColumnValues(idKeys, columns3, currentRow.getValues(), ids);
          if (!values.isEmpty()) {
              values = "VALUES (" + values + ")";
          } else {
              values = "DEFAULT VALUES";
          }

          String query = String.format("INSERT INTO %s %s %s%s;", tableName, finalColumnNames, values, returningId);
          return new Pair<>(query, returningName.map(name -> id -> currentRow.putIds(name, id)));
      };
  }

  public List<Function<FeatureSql, Pair<String, Optional<Consumer<String>>>>> createJunctionInsert(
          List<FeatureStoreRelation> relationPath, List<Integer> parentRows) {

      FeatureStoreRelation childRelation = relationPath.get(relationPath.size() - 1);

      String table = childRelation.getJunction()
                                  .get();

      String columnNames = String.format("%s,%s", childRelation.getJunctionSource()
                                                               .get(), childRelation.getJunctionTarget()
                                                                                    .get());

      String sourceIdColumn = childRelation.getJunctionSource()
                                           .get()
                                           .replace('_', '.');

      String targetIdColumn = childRelation.getTargetContainer() + '.' + childRelation.getTargetField();

      //NOTE: either use List<FeatureStoreRelation> in nestedRow.getNested or extract path formatting
      //List<String> trail = getTrail(relationPath);
      List<FeatureStoreRelation> parentPath = relationPath.subList(0, relationPath.size() - 1);

      ImmutableList.Builder<Function<FeatureSql, Pair<String, Optional<Consumer<String>>>>> queries = ImmutableList.builder();

      queries.add(nestedRow -> {
          Map<String, String> ids = nestedRow.getNestedObject(toPath(relationPath), parentRows)
                                             .getIds();
          Map<String, String> parentIds = nestedRow.getNestedObject(toPath(parentPath), parentRows)
                                                   .getIds();

          String columnValues = String.format("%s,%s", parentIds.get(sourceIdColumn), ids.get(targetIdColumn));

          return new Pair<>(String.format("INSERT INTO %s (%s) VALUES (%s) RETURNING null;", table, columnNames, columnValues), Optional.empty());
      });

      return queries.build();
  }

  public List<Function<FeatureSql, Pair<String, Optional<Consumer<String>>>>> createForeignKeyUpdate(
          List<FeatureStoreRelation> relationPath, List<Integer> parentRows) {

      FeatureStoreRelation parentRelation = relationPath.get(relationPath.size() - 1);

      String table = parentRelation.getSourceContainer();
      String refKey = table + "." + parentRelation.getSourceSortKey();
      String column = parentRelation.getSourceField();
      String columnKey = parentRelation.getTargetContainer() + "." + parentRelation.getTargetField();

      //NOTE: either use List<FeatureStoreRelation> in nestedRow.getNested or extract path formatting
      //List<String> trail = getTrail(relationPath);

      List<FeatureStoreRelation> parentPath = relationPath.subList(0, relationPath.size() - 1);

      return ImmutableList.of(nestedRow -> {
          Map<String, String> ids = nestedRow.getNestedObject(toPath(relationPath), parentRows)
                                             .getIds();
          Map<String, String> parentIds = nestedRow.getNestedObject(toPath(parentPath), parentRows)
                                                   .getIds();

          return new Pair<>(String.format("UPDATE %s SET %s=%s WHERE id=%s RETURNING null;", table, column, ids.get(columnKey), parentIds.get(refKey)), Optional.empty());
      });
  }

  //NOTE: from syntax
  //NOTE: separate valuePaths and idPaths
  String getColumnValues(List<String> idKeys, List<String> columnNames, Map<String, String> values,
                         Map<String, String> ids) {

      List<String> columnValues = Stream.concat(
              idKeys.stream()
                    .map(ids::get),
              columnNames.stream()
                         .map(values::get))
                                        .collect(Collectors.toList());

      for (int j = 0; j < columnNames.size(); j++) {
          if (columnNames.get(j)
                         .startsWith("ST_AsText(ST_ForcePolygonCCW(")) {
              columnValues.set(j, "ST_ForcePolygonCW(ST_GeomFromText(" + columnValues.get(j) + ",25832))"); // NOTE: srid from config
              break;
          }
      }

      return columnValues.stream()
                         .collect(Collectors.joining(","));
  }

  private List<String> toPath(List<FeatureStoreRelation> relationPath) {
      if (relationPath.isEmpty()) {
          return ImmutableList.of();
      }

      return Stream.concat(
              Stream.of(relationPath.get(0).getSourceContainer()),
              relationPath.stream()
                                     .flatMap(relation -> {
                                         if (relation.getJunction()
                                                     .isPresent()) {
                                             return Stream.of(String.format("[%s=%s]%s", relation.getSourceField(), relation.getJunctionSource()
                                                                                                                            .get(), relation.getJunction()
                                                                                                                                            .get()),
                                                     String.format("[%s=%s]%s", relation.getJunctionTarget()
                                                                                        .get(), relation.getTargetField(), relation.getTargetContainer()));
                                         }
                                         return Stream.of(String.format("[%s=%s]%s", relation.getSourceField(), relation.getTargetField(), relation.getTargetContainer()));
                                     }))
                   .collect(Collectors.toList());
  }*/
}
