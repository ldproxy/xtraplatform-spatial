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
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.immutables.value.Value;
import org.immutables.value.Value.Modifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.CouplingBetweenObjects")
public class FeatureEncoderSql
    extends FeatureTokenEncoderBaseSimple<
        SqlQuerySchema,
        SqlQueryMapping,
        ModifiableContext<SqlQuerySchema, SqlQueryMapping>,
        FeatureDataSql> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureEncoderSql.class);

  private final SqlQueryMapping mapping;
  private final EpsgCrs inputCrs;
  private final EpsgCrs nativeCrs;
  private final CrsTransformerFactory crsTransformerFactory;
  private final Optional<ZoneId> timeZone;
  private final Map<String, JsonBuilder> jsonColumns;
  private final boolean isPatch;

  private ModifiableFeatureDataSql currentFeature;
  private Tuple<SqlQuerySchema, SqlQueryColumn> currentJsonColumn;
  private JsonBuilder currentJson;
  private Consumer<String> currentJsonSetter;

  // Junction table targeted by the currently open VALUE_ARRAY, or null when no junction-backed
  // VALUE_ARRAY is open. Set on onArrayStart when the array's value column lives on a non-current
  // table; each onValue inside the array becomes its own junction row.
  private SqlQuerySchema currentArrayJunctionTable;

  public FeatureEncoderSql(
      SqlQueryMapping mapping,
      EpsgCrs inputCrs,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Optional<ZoneId> timeZone,
      Optional<String> nullValue) {
    super();
    this.mapping = mapping;
    this.inputCrs = inputCrs;
    this.crsTransformerFactory = crsTransformerFactory;
    this.nativeCrs = nativeCrs;
    this.timeZone = timeZone;
    this.jsonColumns = new LinkedHashMap<>();
    this.isPatch = nullValue.isPresent();
  }

  @Modifiable
  @Value.Style(deepImmutablesDetection = true)
  interface FeatureEncoderSqlContext extends ModifiableContext<SqlQuerySchema, SqlQueryMapping> {}

  @Override
  public void onStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  @SuppressWarnings("PMD.NullAssignment")
  public void onFeatureStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    currentFeature = ModifiableFeatureDataSql.create().setMapping(mapping);
    currentFeature.addRow(mapping.getMainTable());
    if (!isPatch) {
      currentJsonColumn = null;
      jsonColumns.clear();
    }
    currentArrayJunctionTable = null;
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onFeatureStart: {}", context.pathAsString());
    }
  }

  @Override
  public void onFeatureEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onFeatureEnd: {}", context.pathAsString());
    }

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

    if (LOGGER.isTraceEnabled()) {
      currentFeature
          .getRows()
          .forEach(
              row -> LOGGER.trace("push: {} {}", row.first().getFullPathAsString(), row.second()));
    }

    push(currentFeature);
  }

  @Override
  public void onObjectStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onObjectStart: {}", context.pathAsString());

      mapping.getMainSchema().getAllObjects().stream()
          .filter(schema -> Objects.equals(schema.getFullPath(), context.path()))
          .findFirst()
          .ifPresent(
              schema -> LOGGER.trace("onObjectStart: {} {}", context.pathAsString(), schema));
    }

    Optional<SqlQuerySchema> tableSchema = mapping.getTableForObject(context.pathAsString());

    if (tableSchema.isPresent()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("onObjectStart: table found for {}", context.pathAsString());
      }

      currentFeature.addRow(tableSchema.get());
      return;
    }

    Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> column =
        mapping.getColumnForValue(context.pathAsString(), Scope.W);

    if (column.isPresent() && checkJson(column.get())) {
      currentJson.openObject(context.path());

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("onObjectStart: JSON {}", context.pathAsString());
      }
      return;
    }

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onObjectStart: no table found for {}", context.pathAsString());
    }
  }

  @Override
  public void onObjectEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onObjectEnd: {}", context.pathAsString());
    }

    Optional<SqlQuerySchema> tableSchema = mapping.getTableForObject(context.pathAsString());

    if (tableSchema.isPresent()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("onObjectEnd: table found for {}", context.pathAsString());
      }

      currentFeature.closeRow(tableSchema.get());
      return;
    }

    Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> column =
        mapping.getColumnForValue(context.pathAsString(), Scope.W);

    if (column.isPresent() && checkJson(column.get())) {
      currentJson.closeObject(context.path());

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("onObjectEnd: JSON {}", context.pathAsString());
      }
      return;
    }

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onObjectEnd: no table found for {}", context.pathAsString());
    }
  }

  @Override
  public void onArrayStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onArrayStart: {}", context.pathAsString());
    }

    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        .ifPresentOrElse(
            column -> {
              if (checkJson(column)) {
                currentJson.openArray(context.path());

                if (LOGGER.isTraceEnabled()) {
                  LOGGER.trace("onArrayStart: JSON {}", context.pathAsString());
                }
                return;
              }
              // VALUE_ARRAY whose value column lives on a junction table other than the row at the
              // top of the stack. Record the table here and defer the per-element addRow to
              // onValue, so each array element becomes its own row — mirroring how OBJECT_ARRAY
              // junctions get a row per member via onObjectStart.
              if (!currentFeature.isCurrent(column.first())) {
                currentArrayJunctionTable = column.first();
                if (LOGGER.isTraceEnabled()) {
                  LOGGER.trace(
                      "onArrayStart: junction {} {}",
                      context.pathAsString(),
                      column.first().getFullPathAsString());
                }
              }
            },
            () -> {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("onArrayStart: JSON {} not found in mapping", context.pathAsString());
              }
            });
  }

  @Override
  @SuppressWarnings("PMD.NullAssignment")
  public void onArrayEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onArrayEnd: {}", context.pathAsString());
    }

    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        .ifPresentOrElse(
            column -> {
              if (checkJson(column)) {
                currentJson.closeArray(context.path());

                if (LOGGER.isTraceEnabled()) {
                  LOGGER.trace("onArrayEnd: JSON {}", context.pathAsString());
                }
              }
            },
            () -> {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("onArrayEnd: JSON {} not found in mapping", context.pathAsString());
              }
            });

    currentArrayJunctionTable = null;
  }

  @Override
  public void onGeometry(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    Geometry<?> geometry = context.geometry();

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("geometry: {} {}", context.pathAsString(), geometry);
    }

    // A geometry property that is mapped to its own writable column under its property path
    // (e.g. a per-CRS position variant) takes precedence over the primary geometry. The column's
    // storage CRS — the WKT/WKB operation parameter, set from the schema's `crs` option —
    // determines the transformation target and the SRID of the literal; without the option this
    // is the provider's nativeCrs, preserving the previous behavior.
    Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> columnForPath =
        mapping
            .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
            .filter(
                c ->
                    c.second().hasOperation(SqlQueryColumn.Operation.WKT)
                        || c.second().hasOperation(SqlQueryColumn.Operation.WKB));

    columnForPath
        .or(
            () ->
                mapping
                    .getColumnForPrimaryGeometry()
                    .filter(c -> mapping.getSchemaForPrimaryGeometry().isPresent()))
        .ifPresentOrElse(
            column -> {
              EpsgCrs storageCrs = storageCrs(column.second());
              String value = toWkt(geometry, transformerFor(geometry, storageCrs), storageCrs);

              currentFeature.addColumn(column.first(), column.second(), value);

              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("onGeometry: {} {}", context.pathAsString(), value);
              }
            },
            () -> {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("onGeometry: {} not found in mapping", context.pathAsString());
              }
            });
  }

  private EpsgCrs storageCrs(SqlQueryColumn column) {
    SqlQueryColumn.Operation op =
        column.hasOperation(SqlQueryColumn.Operation.WKB)
            ? SqlQueryColumn.Operation.WKB
            : SqlQueryColumn.Operation.WKT;
    List<String> parameters = column.getOperationParameters(op);
    if (parameters.isEmpty()) {
      return nativeCrs;
    }
    return EpsgCrs.of(
        Integer.parseInt(parameters.get(0)),
        parameters.size() > 1 ? EpsgCrs.Force.valueOf(parameters.get(1)) : EpsgCrs.Force.NONE);
  }

  // The transformation source is the CRS the geometry actually arrived in (set by the format
  // decoder from srsName or the request CRS), falling back to the request CRS for decoders that
  // do not tag geometries.
  private Optional<CrsTransformer> transformerFor(Geometry<?> geometry, EpsgCrs storageCrs) {
    EpsgCrs sourceCrs = geometry.getCrs().orElse(inputCrs);
    if (Objects.equals(sourceCrs, storageCrs)) {
      return Optional.empty();
    }
    return crsTransformerFactory.getTransformer(sourceCrs, storageCrs);
  }

  @Override
  @SuppressWarnings("PMD.CognitiveComplexity")
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
                value = toTimeZone(context.pathAsString(), value, timeZone.get());
              }

              if (checkJson(column)) {
                value = Objects.nonNull(value) ? value.replaceAll("'", "''") : value;
                // TODO: does this use the sql name or json name?
                currentJson.addValue(context.path(), value);

                if (LOGGER.isTraceEnabled()) {
                  LOGGER.trace("onValue: JSON {} {}", context.pathAsString(), value);
                }
                return;
              }

              // Numeric/boolean values are validated and re-rendered (never inlined as raw request
              // text); string/date values are quoted with quote-doubling. A null stays null so the
              // downstream row renderer emits SQL NULL. See SqlLiterals.
              value =
                  Objects.nonNull(value)
                      ? SqlLiterals.forType(column.second().getType(), value)
                      : value;

              boolean junctionElement =
                  currentArrayJunctionTable != null
                      && Objects.equals(column.first(), currentArrayJunctionTable);
              if (junctionElement) {
                currentFeature.addRow(column.first());
              }
              currentFeature.addColumn(column.first(), column.second(), value);
              if (junctionElement) {
                currentFeature.closeRow(column.first());
              }

              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("onValue: {} {}", context.pathAsString(), value);
              }
            },
            () -> {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("onValue: {} not found in mapping", context.pathAsString());
              }
            });
  }

  @Override
  public Class<? extends ModifiableContext<SqlQuerySchema, SqlQueryMapping>> getContextInterface() {
    return FeatureEncoderSqlContext.class;
  }

  @Override
  public ModifiableContext<SqlQuerySchema, SqlQueryMapping> createContext() {
    return ModifiableFeatureEncoderSqlContext.create()
        .setType(mapping.getMainSchema().getName())
        .setMappings(Map.of(mapping.getMainSchema().getName(), mapping));
  }

  private boolean checkJson(Tuple<SqlQuerySchema, SqlQueryColumn> column) {
    if (column.second().hasOperation(Operation.CONNECTOR)
        && "JSON".equals(column.second().getOperationParameter(Operation.CONNECTOR, ""))) {
      if (currentJsonColumn == null) {
        this.currentJsonColumn = column;
        this.jsonColumns.put(currentJsonColumn.second().getPathSegment(), new JsonBuilder());
        this.currentJson = jsonColumns.get(currentJsonColumn.second().getPathSegment());
        this.currentJsonSetter =
            currentFeature.addLazyColumn(currentJsonColumn.first(), currentJsonColumn.second());
      }

      return true;
    }
    return false;
  }

  private static String toTimeZone(String path, String value, ZoneId timeZone) {
    try {
      DateTimeFormatter parser = DateTimeFormatter.ISO_DATE_TIME;

      LocalDateTime ta = parser.parse(value, LocalDateTime::from);

      ZonedDateTime instant = ta.atZone(ZoneId.of("UTC")).withZoneSameInstant(timeZone);

      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

      String newValue = formatter.format(instant) + "Z";

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "onValue: {} transformed datetime value from '{}' to '{}'", path, value, newValue);
      }

      return newValue;
    } catch (DateTimeException e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("Error while parsing datetime value for {}: {}", path, e.getMessage());
      }
    }

    return value;
  }

  private static String toWkt(
      Geometry<?> geometry, Optional<CrsTransformer> crsTransformer, EpsgCrs storageCrs) {

    Geometry<?> transformedGeometry =
        crsTransformer.isPresent()
            ? geometry.accept(
                new CoordinatesTransformer(
                    ImmutableCrsTransform.of(Optional.empty(), crsTransformer.get())))
            : geometry;

    String wkt;
    try {
      wkt = new GeometryEncoderWkt().encode(transformedGeometry);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    // TODO: functions from Dialect
    String result = String.format("ST_GeomFromText('%s',%s)", wkt, storageCrs.getCode());

    if (transformedGeometry.getType() == GeometryType.POLYGON
        || transformedGeometry.getType() == GeometryType.MULTI_POLYGON) {
      result = String.format("ST_ForcePolygonCW(%s)", result);
    }

    return result;
  }
}
