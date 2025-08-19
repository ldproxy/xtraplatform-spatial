/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.MappingRule.Scope;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenEncoderBaseSimple;
import de.ii.xtraplatform.features.json.domain.JsonBuilder;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt;
import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformer;
import de.ii.xtraplatform.geometries.domain.transform.ImmutableCrsTransform;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.immutables.value.Value;
import org.immutables.value.Value.Modifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureEncoderSql
    extends FeatureTokenEncoderBaseSimple<
        SqlQuerySchema,
        SqlQueryMapping,
        ModifiableContext<SqlQuerySchema, SqlQueryMapping>,
        FeatureDataSql> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureEncoderSql.class);

  private final SqlQueryMapping mapping;
  private final EpsgCrs nativeCrs;
  private final Optional<CrsTransformer> crsTransformer;
  private final Optional<ZoneId> timeZone;
  private final Optional<String> nullValue;
  private Map<String, JsonBuilder> jsonColumns;
  private final boolean isPatch;
  private final boolean trace;

  private ModifiableFeatureDataSql currentFeature;
  private Tuple<SqlQuerySchema, SqlQueryColumn> currentJsonColumn;
  private JsonBuilder currentJson;
  private Consumer<String> currentJsonSetter;

  public FeatureEncoderSql(
      SqlQueryMapping mapping,
      EpsgCrs inputCrs,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Optional<ZoneId> timeZone,
      Optional<String> nullValue) {
    this.mapping = mapping;
    this.crsTransformer = crsTransformerFactory.getTransformer(inputCrs, nativeCrs);
    this.nativeCrs = nativeCrs;
    this.timeZone = timeZone;
    this.nullValue = nullValue;
    this.jsonColumns = new LinkedHashMap<>();
    this.isPatch = nullValue.isPresent();
    this.trace = false;
  }

  @Modifiable
  @Value.Style(deepImmutablesDetection = true)
  interface FeatureEncoderSqlContext extends ModifiableContext<SqlQuerySchema, SqlQueryMapping> {}

  @Override
  public void onStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onFeatureStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    currentFeature = ModifiableFeatureDataSql.create().setMapping(mapping);
    currentFeature.addRow(mapping.getMainTable());
    if (!isPatch) {
      currentJsonColumn = null;
      jsonColumns.clear();
    }
    if (trace) LOGGER.debug("onFeatureStart: {}", context.pathAsString());
  }

  @Override
  public void onFeatureEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (trace) LOGGER.debug("onFeatureEnd: {}", context.pathAsString());

    if (currentJsonColumn != null) {
      try {
        currentJsonSetter.accept("'" + currentJson.build() + "'");
      } catch (IOException e) {
        LOGGER.error(
            "Error while serializing JSON column {}: {}",
            currentJsonColumn.second().getName(),
            e.getMessage());
      }
    }

    currentFeature
        .getRows()
        .forEach(
            row -> {
              if (trace)
                LOGGER.debug("push: {} {}", row.first().getFullPathAsString(), row.second());
            });

    push(currentFeature);
  }

  @Override
  public void onObjectStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (trace) LOGGER.debug("onObjectStart: {}", context.pathAsString());

    mapping.getMainSchema().getAllObjects().stream()
        .filter(schema -> Objects.equals(schema.getFullPath(), context.path()))
        .findFirst()
        .ifPresent(
            schema -> {
              if (trace) LOGGER.debug("onObjectStart: {} {}", context.pathAsString(), schema);
            });

    Optional<SqlQuerySchema> tableSchema = mapping.getTableForObject(context.pathAsString());

    if (tableSchema.isPresent()) {
      if (trace) LOGGER.debug("onObjectStart: table found for {}", context.pathAsString());

      currentFeature.addRow(tableSchema.get());
      return;
    }

    Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> column =
        mapping.getColumnForValue(context.pathAsString(), Scope.W);

    if (column.isPresent() && checkJson(column.get())) {
      currentJson.openObject(context.path());

      if (trace) LOGGER.debug("onObjectStart: JSON {}", context.pathAsString());
      return;
    }

    if (trace) LOGGER.warn("onObjectStart: no table found for {}", context.pathAsString());
  }

  @Override
  public void onObjectEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (trace) LOGGER.debug("onObjectEnd: {}", context.pathAsString());

    Optional<SqlQuerySchema> tableSchema = mapping.getTableForObject(context.pathAsString());

    if (tableSchema.isPresent()) {
      if (trace) LOGGER.debug("onObjectEnd: table found for {}", context.pathAsString());

      currentFeature.closeRow(tableSchema.get());
      return;
    }

    Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> column =
        mapping.getColumnForValue(context.pathAsString(), Scope.W);

    if (column.isPresent() && checkJson(column.get())) {
      currentJson.closeObject(context.path());

      if (trace) LOGGER.debug("onObjectEnd: JSON {}", context.pathAsString());
      return;
    }

    if (trace) LOGGER.warn("onObjectEnd: no table found for {}", context.pathAsString());
  }

  @Override
  public void onArrayStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (trace) LOGGER.debug("onArrayStart: {} {}", context.pathAsString());

    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        .ifPresentOrElse(
            column -> {
              if (checkJson(column)) {
                currentJson.openArray(context.path());

                if (trace) LOGGER.debug("onArrayStart: JSON {}", context.pathAsString());
              }
            },
            () -> {
              if (trace)
                LOGGER.warn("onArrayStart: JSON {} not found in mapping", context.pathAsString());
            });
  }

  @Override
  public void onArrayEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (trace) LOGGER.debug("onArrayEnd: {} {}", context.pathAsString());

    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        .ifPresentOrElse(
            column -> {
              if (checkJson(column)) {
                currentJson.closeArray(context.path());

                if (trace) LOGGER.debug("onArrayEnd: JSON {}", context.pathAsString());
              }
            },
            () -> {
              if (trace)
                LOGGER.warn("onArrayEnd: JSON {} not found in mapping", context.pathAsString());
            });
  }

  @Override
  public void onGeometry(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    Geometry<?> geometry = context.geometry();

    if (trace) {
      LOGGER.debug("geometry: {} {}", context.pathAsString(), geometry);
    }

    mapping
        .getColumnForPrimaryGeometry()
        .ifPresentOrElse(
            column -> {
              mapping
                  .getSchemaForPrimaryGeometry()
                  .ifPresentOrElse(
                      schema -> {
                        String value = toWkt(geometry, crsTransformer, nativeCrs);

                        currentFeature.addColumn(column.first(), column.second(), value);

                        if (trace) {
                          LOGGER.debug("onGeometry: {} {}", context.pathAsString(), value);
                        }
                      },
                      () -> {
                        if (trace) {
                          LOGGER.warn(
                              "onGeometry: {} not found in mapping", context.pathAsString());
                        }
                      });
            },
            () -> {
              if (trace) {
                LOGGER.warn("onGeometry: {} not found in mapping", context.pathAsString());
              }
            });
  }

  @Override
  public void onValue(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        // TODO: this is only correct for geojson, should be handled in the decoder
        .or(() -> "id".equals(context.pathAsString()) ? mapping.getColumnForId() : Optional.empty())
        .ifPresentOrElse(
            column -> {
              String value = context.value();

              if (timeZone.isPresent()
                  && column.second().getType() == Type.DATETIME
                  && Objects.nonNull(value)) {
                value = toTimeZone(context.pathAsString(), value, timeZone.get(), trace);
              }

              if (checkJson(column)) {
                value = Objects.nonNull(value) ? value.replaceAll("'", "''") : value;
                // TODO: does this use the sql name or json name?
                currentJson.addValue(context.path(), value);

                if (trace) LOGGER.debug("onValue: JSON {} {}", context.pathAsString(), value);
                return;
              }

              boolean needsQuotes =
                  column.second().getType() == SchemaBase.Type.STRING
                      || column.second().getType() == SchemaBase.Type.DATETIME
                      || column.second().getType() == SchemaBase.Type.DATE;
              /* (schemaSql.getType() == SchemaBase.Type.FEATURE_REF
              && schemaSql.getValueType().orElse(SchemaBase.Type.STRING)
              == SchemaBase.Type.STRING)*/

              value =
                  needsQuotes && Objects.nonNull(value)
                      ? String.format("'%s'", value.replaceAll("'", "''"))
                      : value;

              currentFeature.addColumn(column.first(), column.second(), value);

              if (trace) LOGGER.debug("onValue: {} {}", context.pathAsString(), value);
            },
            () -> {
              if (trace) LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
            });
  }

  @Override
  public Class<? extends ModifiableContext<SqlQuerySchema, SqlQueryMapping>> getContextInterface() {
    return FeatureEncoderSqlContext.class;
  }

  @Override
  public ModifiableContext<SqlQuerySchema, SqlQueryMapping> createContext() {
    return ModifiableFeatureEncoderSqlContext.create();
  }

  private boolean checkJson(Tuple<SqlQuerySchema, SqlQueryColumn> column) {
    if (column.second().hasOperation(Operation.CONNECTOR)) {
      if (column.second().getOperationParameter(Operation.CONNECTOR, "").equals("JSON")) {
        if (currentJsonColumn == null) {
          this.currentJsonColumn = column;
          this.jsonColumns.put(currentJsonColumn.second().getPathSegment(), new JsonBuilder());
          this.currentJson = jsonColumns.get(currentJsonColumn.second().getPathSegment());
          this.currentJsonSetter =
              currentFeature.addLazyColumn(currentJsonColumn.first(), currentJsonColumn.second());
        }

        return true;
      }
    }
    return false;
  }

  private static String toTimeZone(String path, String value, ZoneId timeZone, boolean trace) {
    try {
      DateTimeFormatter parser = DateTimeFormatter.ISO_DATE_TIME;

      LocalDateTime ta = parser.parse(value, LocalDateTime::from);

      ZonedDateTime instant = ta.atZone(ZoneId.of("UTC")).withZoneSameInstant(timeZone);

      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

      String newValue = formatter.format(instant) + "Z";

      if (trace) {
        LOGGER.debug(
            "onValue: {} transformed datetime value from '{}' to '{}'", path, value, newValue);
      }

      return newValue;
    } catch (Throwable e) {
      LOGGER.warn("Error while parsing datetime value for {}: {}", path, e.getMessage());
    }

    return value;
  }

  private static String toWkt(
      Geometry<?> geometry, Optional<CrsTransformer> crsTransformer, EpsgCrs nativeCrs) {

    if (crsTransformer.isPresent()) {
      geometry =
          geometry.accept(
              new CoordinatesTransformer(
                  ImmutableCrsTransform.of(Optional.empty(), crsTransformer.get())));
    }

    String wkt;
    try {
      wkt = new GeometryEncoderWkt().encode(geometry);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    // TODO: functions from Dialect
    String result = String.format("ST_GeomFromText('%s',%s)", wkt, nativeCrs.getCode());

    if (geometry.getType() == GeometryType.POLYGON
        || geometry.getType() == GeometryType.MULTI_POLYGON) {
      result = String.format("ST_ForcePolygonCW(%s)", result);
    }

    return result;
  }
}
