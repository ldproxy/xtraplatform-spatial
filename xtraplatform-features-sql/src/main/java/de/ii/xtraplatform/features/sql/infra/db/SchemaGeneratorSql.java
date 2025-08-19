/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Scope;
import de.ii.xtraplatform.features.domain.SchemaGenerator;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasic;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter.GeoInfo;
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.WktWkbGeometryType;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.ColumnDataType;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;

public class SchemaGeneratorSql implements SchemaGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGeneratorSql.class);

  private final SqlClientBasic sqlClientBasic;
  private final Connection connection;
  private final SqlSchemaCrawler schemaCrawler;
  private final SqlDbmsAdapter dbmsAdapter;

  public SchemaGeneratorSql(SqlClientBasic sqlClientBasic) {
    this.sqlClientBasic = sqlClientBasic;
    this.connection = sqlClientBasic.getConnection();
    this.schemaCrawler = new SqlSchemaCrawler(connection);
    this.dbmsAdapter = sqlClientBasic.getDbmsAdapter();
  }

  @Override
  public Map<String, List<String>> analyze() {
    try {
      Catalog catalog =
          schemaCrawler.getCatalog(dbmsAdapter.getSystemSchemas(), dbmsAdapter.getSystemTables());

      return catalog.getSchemas().stream()
          .map(
              schema ->
                  Map.entry(
                      Objects.requireNonNullElse(schema.getName(), ""),
                      catalog.getTables(schema).stream()
                          .map(Table::getName)
                          .collect(Collectors.toList())))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (Throwable e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stacktrace:", e);
      }
      throw new IllegalStateException("Could not crawl schema", e);
    }
  }

  @Override
  public List<FeatureSchema> generate(
      Map<String, List<String>> types, Consumer<Map<String, List<String>>> tracker) {
    try {
      LOGGER.debug("Crawling SQL schema");

      Map<String, List<String>> progress = new LinkedHashMap<>();
      Map<String, GeoInfo> geoInfo = sqlClientBasic.getGeoInfo();

      List<FeatureSchema> featureTypes =
          types.entrySet().stream()
              .flatMap(
                  schema ->
                      schema.getValue().stream()
                          .map(
                              tableName -> {
                                track(progress, tracker, schema.getKey(), tableName);

                                try {
                                  Catalog catalog =
                                      schemaCrawler.getCatalog(schema.getKey(), tableName);

                                  Table table =
                                      catalog
                                          .lookupSchema(schema.getKey())
                                          .flatMap(s -> catalog.lookupTable(s, tableName))
                                          .map(t -> (Table) t)
                                          .orElseThrow(
                                              () ->
                                                  new RuntimeException(
                                                      String.format(
                                                          "Could not crawl schema for %s.%s",
                                                          schema.getKey(), tableName)));

                                  return getFeatureType(table, geoInfo);
                                } catch (Throwable e) {
                                  return null;
                                }
                              }))
              .collect(Collectors.toList());

      LOGGER.debug("Finished crawling SQL schema");

      return featureTypes;
    } catch (Throwable e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stacktrace:", e);
      }
      throw new IllegalStateException("Could not crawl schema", e);
    }
  }

  private static void track(
      Map<String, List<String>> progress,
      Consumer<Map<String, List<String>>> tracker,
      String schema,
      String table) {
    boolean changed = false;

    if (!progress.containsKey(schema)) {
      progress.put(schema, new ArrayList<>());
      changed = true;
    }
    if (!progress.get(schema).contains(table)) {
      progress.get(schema).add(table);
      changed = true;
    }
    if (changed) {
      tracker.accept(progress);
    }
  }

  private List<FeatureSchema> getFeatureTypes(
      Catalog catalog, List<String> includeTables, Map<String, GeoInfo> geometryInfos) {
    ImmutableList.Builder<FeatureSchema> featureTypes = new ImmutableList.Builder<>();

    for (final Schema schema : catalog.getSchemas()) {

      SchemaInfo schemaInfo = new SchemaInfo(catalog.getTables(schema));

      for (final Table table : catalog.getTables(schema)) {
        if (!includeTables.isEmpty() && !includeTables.contains(table.getName())) {
          continue;
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Generating type '{}'", table.getName());
        }

        ImmutableFeatureSchema.Builder featureType =
            new ImmutableFeatureSchema.Builder()
                .name(table.getName())
                .sourcePath("/" + table.getName().toLowerCase());

        boolean idFound = false;

        for (final Column column : table.getColumns()) {
          SchemaBase.Type featurePropertyType = getFeaturePropertyType(column.getColumnDataType());
          if (featurePropertyType != SchemaBase.Type.UNKNOWN) {
            ImmutableFeatureSchema.Builder featureProperty =
                new ImmutableFeatureSchema.Builder()
                    .name(column.getName())
                    .sourcePath(column.getName())
                    .type(featurePropertyType);
            if (!idFound && schemaInfo.isColumnUnique(column.getName(), table.getName(), false)) {
              featureProperty.role(SchemaBase.Role.ID);
              idFound = true;
            }
            if (schemaInfo.isColumnReadOnly(column.getName(), table.getName())) {
              featureProperty.excludedScopes(ImmutableList.of(Scope.RECEIVABLE));
            }
            if (featurePropertyType == SchemaBase.Type.GEOMETRY) {
              if (!geometryInfos.containsKey(table.getName())) {
                continue;
              }
              GeoInfo geometryInfo = geometryInfos.get(table.getName());

              // if srid=0, do not set, will use default
              try {
                int srid = Integer.parseInt(geometryInfo.getSrid());
                if (srid > 0) {
                  featureProperty.additionalInfo(
                      ImmutableMap.of(
                          "crs", String.valueOf(srid), "force", geometryInfo.getForce()));
                }
              } catch (Throwable e) {
                // ignore
              }

              featureProperty.geometryType(
                  WktWkbGeometryType.valueOf(geometryInfo.getType()).toGeometryType());
            }
            featureType.putPropertyMap(column.getName(), featureProperty.build());
          }
        }

        ImmutableFeatureSchema featureSchema = featureType.build();

        if (featureSchema.getProperties().stream().noneMatch(FeatureSchema::isId)) {
          LOGGER.warn(
              "No primary key or unique column found for table '{}', you have to adjust the type configuration manually.",
              table.getName());
        }

        featureTypes.add(featureSchema);
      }
    }
    return featureTypes.build();
  }

  private FeatureSchema getFeatureType(Table table, Map<String, GeoInfo> geometryInfos) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Generating type '{}'", table.getName());
    }

    SchemaInfo schemaInfo = new SchemaInfo(List.of(table));

    ImmutableFeatureSchema.Builder featureType =
        new ImmutableFeatureSchema.Builder()
            .name(table.getName())
            .sourcePath("/" + table.getName().toLowerCase());

    boolean idFound = false;

    for (final Column column : table.getColumns()) {
      SchemaBase.Type featurePropertyType = getFeaturePropertyType(column.getColumnDataType());
      if (featurePropertyType != SchemaBase.Type.UNKNOWN) {
        ImmutableFeatureSchema.Builder featureProperty =
            new ImmutableFeatureSchema.Builder()
                .name(column.getName())
                .sourcePath(column.getName())
                .type(featurePropertyType);
        if (!idFound && schemaInfo.isColumnUnique(column.getName(), table.getName(), false)) {
          featureProperty.role(SchemaBase.Role.ID);
          idFound = true;
        }
        if (schemaInfo.isColumnReadOnly(column.getName(), table.getName())) {
          if (featurePropertyType == SchemaBase.Type.GEOMETRY) {
            featureProperty.addExcludedScopes(Scope.RECEIVABLE, Scope.SORTABLE);
          } else {
            featureProperty.addExcludedScopes(Scope.RECEIVABLE);
          }
        }
        if (featurePropertyType == SchemaBase.Type.GEOMETRY) {
          if (!geometryInfos.containsKey(table.getName())) {
            continue;
          }
          GeoInfo geometryInfo = geometryInfos.get(table.getName());

          // if srid=0, do not set, will use default
          try {
            int srid = Integer.parseInt(geometryInfo.getSrid());
            if (srid > 0) {
              featureProperty.additionalInfo(
                  ImmutableMap.of("crs", String.valueOf(srid), "force", geometryInfo.getForce()));
            }
          } catch (Throwable e) {
            // ignore
          }

          featureProperty.geometryType(
              WktWkbGeometryType.valueOf(geometryInfo.getType()).toGeometryType());
        }
        featureType.putPropertyMap(column.getName(), featureProperty.build());
      }
    }

    FeatureSchema featureSchema = featureType.build();

    if (featureSchema.getProperties().stream().noneMatch(FeatureSchema::isId)) {
      LOGGER.warn(
          "No primary key or unique column found for table '{}', you have to adjust the type configuration manually.",
          table.getName());
    }

    return featureSchema;
  }

  private SchemaBase.Type getFeaturePropertyType(ColumnDataType columnDataType) {
    // TODO: pass GeoInfo to determine geo columns
    try {
      if ("GEOMETRY".equals(columnDataType.getName())
          || WktWkbGeometryType.valueOf(columnDataType.getName()) != WktWkbGeometryType.NONE) {
        return SchemaBase.Type.GEOMETRY;
      }
    } catch (Exception ignore) {
      // ignore, not a geometry
    }

    return switch (columnDataType.getJavaSqlType().getJavaSqlTypeGroup()) {
      case bit -> SchemaBase.Type.BOOLEAN;
      case character -> SchemaBase.Type.STRING;
      case integer -> SchemaBase.Type.INTEGER;
      case real -> SchemaBase.Type.FLOAT;
      case temporal -> SchemaBase.Type.DATETIME;
      default -> SchemaBase.Type.UNKNOWN;
    };
  }

  @Override
  public void close() throws IOException {
    try {
      schemaCrawler.close();
      connection.close();
    } catch (SQLException exception) {
      throw new IOException(exception);
    }
  }
}
