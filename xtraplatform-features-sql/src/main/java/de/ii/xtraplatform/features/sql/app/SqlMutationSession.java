/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenSource;
import de.ii.xtraplatform.features.domain.FeatureTransactions;
import de.ii.xtraplatform.features.domain.ImmutableMutationResult;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.FeatureTokenStatsCollector;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlSession;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.streams.domain.Reactive.Source;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous {@link FeatureTransactions.Session} backed by a single JDBC connection. Reuses {@link
 * FeatureMutationsSql} to derive insert/delete statements and executes them sequentially on the
 * underlying {@link SqlSession} so that all mutations participate in one transaction.
 */
public class SqlMutationSession implements FeatureTransactions.Session {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlMutationSession.class);

  private final SqlSession sqlSession;
  private final Map<String, List<SqlQueryMapping>> queryMappings;
  private final FeatureMutationsSql featureMutationsSql;
  private final EpsgCrs nativeCrs;
  private final CrsTransformerFactory crsTransformerFactory;
  private final Optional<ZoneId> nativeTimeZone;
  private final Reactive.Runner streamRunner;

  public SqlMutationSession(
      SqlSession sqlSession,
      Map<String, List<SqlQueryMapping>> queryMappings,
      FeatureMutationsSql featureMutationsSql,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Optional<ZoneId> nativeTimeZone,
      Reactive.Runner streamRunner) {
    this.sqlSession = sqlSession;
    this.queryMappings = queryMappings;
    this.featureMutationsSql = featureMutationsSql;
    this.nativeCrs = nativeCrs;
    this.crsTransformerFactory = crsTransformerFactory;
    this.nativeTimeZone = nativeTimeZone;
    this.streamRunner = streamRunner;
  }

  @Override
  public FeatureTransactions.MutationResult createFeatures(
      String featureType,
      FeatureTokenSource featureTokenSource,
      EpsgCrs crs,
      Optional<String> featureId) {
    return writeFeatures(
        FeatureTransactions.MutationResult.Type.CREATE,
        featureType,
        featureTokenSource,
        featureId,
        crs,
        false);
  }

  // Multi-source CREATE: drains every source into one `collected` list against this session's
  // single underlying transaction, then runs the existing cross-feature batched write. This is
  // where the wfs:Insert N-feature win actually lands — the executor accumulates up to BATCH_SIZE
  // per-feature sources and calls this overload, so writeFeaturesBatched sees collected.size()>1
  // and runMainInsertsGrouped can fold consecutive same-shape INSERTs into one multi-row INSERT.
  @Override
  public FeatureTransactions.MutationResult createFeatures(
      String featureType, Iterable<FeatureTokenSource> featureTokenSources, EpsgCrs crs) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.CREATE)
            .hasFeatures(false);

    List<FeatureDataSql> collected = new ArrayList<>();
    try {
      for (FeatureTokenSource src : featureTokenSources) {
        drainSource(src, mapping, crs, builder, collected, false);
      }
    } catch (RuntimeException e) {
      return builder.error(translateEncoderError(e)).build();
    }

    if (collected.isEmpty()) {
      return builder.build();
    }

    RowCursor rowCursor = new RowCursor(mapping.getMainTable().getFullPath());
    Optional<
            de.ii.xtraplatform.base.domain.util.Tuple<
                de.ii.xtraplatform.features.sql.domain.SqlQuerySchema,
                de.ii.xtraplatform.features.sql.domain.SqlQueryColumn>>
        roleIdColumn = mapping.getColumnForId();
    String roleIdColumnName = roleIdColumn.map(t -> t.second().getName()).orElse(null);
    de.ii.xtraplatform.features.sql.domain.SqlQuerySchema roleIdTable =
        roleIdColumn.map(de.ii.xtraplatform.base.domain.util.Tuple::first).orElse(null);

    try {
      writeFeaturesBatched(
          collected, rowCursor, Optional.empty(), crs, roleIdTable, roleIdColumnName, builder);
    } catch (RuntimeException e) {
      builder.error(e);
    }

    return builder.build();
  }

  @Override
  public FeatureTransactions.MutationResult updateFeature(
      String featureType,
      String id,
      FeatureTokenSource featureTokenSource,
      EpsgCrs crs,
      boolean partial) {
    return writeFeatures(
        partial
            ? FeatureTransactions.MutationResult.Type.UPDATE
            : FeatureTransactions.MutationResult.Type.REPLACE,
        featureType,
        featureTokenSource,
        Optional.of(id),
        crs,
        partial);
  }

  @Override
  public FeatureTransactions.MutationResult deleteFeature(String featureType, String id) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.DELETE)
            .hasFeatures(false);
    try {
      Supplier<Tuple<String, Consumer<String>>> delete =
          featureMutationsSql.createInstanceDelete(mapping, id);
      Tuple<String, Consumer<String>> tuple = delete.get();
      String returned =
          sqlSession.run(
              ImmutableList.of(tuple::first), ImmutableList.of(tuple.second()), Optional.empty());
      if (returned != null) {
        builder.addIds(returned);
      }
    } catch (RuntimeException e) {
      builder.error(e);
    }
    return builder.build();
  }

  // Property-level partial update. Routes each PropertyUpdate to either the main-table SET path
  // (`UPDATE main SET col = lit WHERE id_col = '<id>'`) or, for VALUE_ARRAY / one-to-many
  // junction-backed properties, a DELETE-existing + INSERT-new pair against the junction table
  // (`DELETE FROM junction WHERE fk IN (SELECT parent_pk FROM main WHERE id_col = ...); INSERT
  // INTO junction (fk, value) SELECT parent_pk, v FROM main, (VALUES ...) v(value) WHERE
  // id_col = ...`). Runs synchronously on the session's connection so prior writes within the
  // same transaction are visible. Geometry properties on the main table are supported via the
  // GeoJSON-to-WKT codec (ST_GeomFromText). M:N junctions, OBJECT_ARRAY/object-FK paths, and
  // FEATURE_REF arrays are not yet supported and rejected with a clear error.
  @Override
  public FeatureTransactions.MutationResult patchFeature(
      String featureType,
      String featureId,
      List<FeatureTransactions.PropertyUpdate> updates,
      EpsgCrs crs) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.UPDATE)
            .hasFeatures(false);
    if (updates.isEmpty()) {
      return builder.build();
    }

    Optional<
            de.ii.xtraplatform.base.domain.util.Tuple<
                de.ii.xtraplatform.features.sql.domain.SqlQuerySchema,
                de.ii.xtraplatform.features.sql.domain.SqlQueryColumn>>
        idColumn = mapping.getColumnForId();
    if (idColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '" + featureType + "' has no id column; cannot patch in place."))
          .build();
    }
    de.ii.xtraplatform.features.sql.domain.SqlQuerySchema mainTable = mapping.getMainTable();
    String mainTableName = mainTable.getName();
    String idColumnName = idColumn.get().second().getName();
    String idLiteral = sqlString(featureId);

    List<String> setClauses = new ArrayList<>();
    // Junction-backed updates: ordered by first-touch path so deterministic SQL ordering.
    java.util.LinkedHashMap<String, JunctionPatch> junctionPatches =
        new java.util.LinkedHashMap<>();

    for (FeatureTransactions.PropertyUpdate update : updates) {
      String joined = String.join(".", update.getPath());
      try {
        // Try column lookup first: scalar/datetime/geometry on the main table, or VALUE_ARRAY's
        // value column on a junction, all surface as a single column.
        Optional<
                de.ii.xtraplatform.base.domain.util.Tuple<
                    de.ii.xtraplatform.features.sql.domain.SqlQuerySchema,
                    de.ii.xtraplatform.features.sql.domain.SqlQueryColumn>>
            resolved =
                mapping.getColumnForValue(
                    joined, de.ii.xtraplatform.features.domain.MappingRule.Scope.W);
        if (resolved.isPresent()) {
          de.ii.xtraplatform.features.sql.domain.SqlQuerySchema table = resolved.get().first();
          de.ii.xtraplatform.features.sql.domain.SqlQueryColumn column = resolved.get().second();
          if (Objects.equals(table.getName(), mainTableName)) {
            String literal = encodeLiteral(column, update.getValue(), crs);
            setClauses.add(column.getName() + " = " + literal);
          } else if (table.isOne2N()) {
            JunctionPatch patch =
                junctionPatches.computeIfAbsent(
                    table.getName(), k -> JunctionPatch.valueArray(table, column));
            patch.appendValues(update.getValue());
          } else {
            return builder
                .error(
                    new IllegalArgumentException(
                        "Property '"
                            + joined
                            + "' is backed by an M:N junction; not yet supported."))
                .build();
          }
          continue;
        }
        // Not a column. May be an OBJECT_ARRAY parent (its children's columns live on a junction).
        // SqlQueryMapping doesn't populate object-schemas, so resolve the parent FeatureSchema by
        // walking the canonical path from the main schema.
        Optional<de.ii.xtraplatform.features.sql.domain.SqlQuerySchema> objectTable =
            mapping.getTableForObject(joined);
        FeatureSchema objectSchema = resolveSchemaByPath(mapping.getMainSchema(), update.getPath());
        if (objectTable.isPresent()
            && objectSchema != null
            && objectSchema.getType()
                == de.ii.xtraplatform.features.domain.SchemaBase.Type.OBJECT_ARRAY
            && objectTable.get().isOne2N()) {
          JunctionPatch patch =
              junctionPatches.computeIfAbsent(
                  objectTable.get().getName(),
                  k -> JunctionPatch.objectArray(objectTable.get(), objectSchema, mapping, joined));
          patch.appendObjectValues(update.getValue());
          continue;
        }
        return builder
            .error(
                new IllegalArgumentException(
                    "Property '"
                        + joined
                        + "' is not a writable column of feature type '"
                        + featureType
                        + "'."))
            .build();
      } catch (IllegalArgumentException e) {
        return builder.error(e).build();
      }
    }

    try {
      if (!setClauses.isEmpty()) {
        String sql =
            "UPDATE "
                + mainTableName
                + " SET "
                + String.join(", ", setClauses)
                + " WHERE "
                + idColumnName
                + " = "
                + idLiteral
                + " RETURNING "
                + idColumnName
                + ";";
        List<String> returned = sqlSession.runReturning(sql);
        if (returned.isEmpty()) {
          return builder
              .error(
                  new IllegalArgumentException(
                      "No feature with id '"
                          + featureId
                          + "' in collection '"
                          + featureType
                          + "'."))
              .build();
        }
        for (String id : returned) {
          builder.addIds(id);
        }
      }

      for (JunctionPatch patch : junctionPatches.values()) {
        runJunctionPatch(patch, mainTableName, idColumnName, idLiteral, crs);
      }

      // No main-table SET ran but at least one junction was patched: confirm the feature exists.
      if (setClauses.isEmpty() && !junctionPatches.isEmpty()) {
        List<String> exists =
            sqlSession.runReturning(
                "SELECT "
                    + idColumnName
                    + " FROM "
                    + mainTableName
                    + " WHERE "
                    + idColumnName
                    + " = "
                    + idLiteral
                    + ";");
        if (exists.isEmpty()) {
          return builder
              .error(
                  new IllegalArgumentException(
                      "No feature with id '"
                          + featureId
                          + "' in collection '"
                          + featureType
                          + "'."))
              .build();
        }
        builder.addIds(featureId);
      }
    } catch (RuntimeException e) {
      builder.error(e);
    }
    return builder.build();
  }

  private void runJunctionPatch(
      JunctionPatch patch,
      String mainTableName,
      String idColumnName,
      String idLiteral,
      EpsgCrs crs) {
    // In SqlQueryJoin (read from the child/junction's perspective): `sourceField` is on the
    // PARENT (its primary/sort key) and `targetField` is on the CHILD (the FK back to parent).
    // See SqlInsertGenerator2 line ~132 (`parent.sourceField` used as the parent sort key) and
    // line ~133 (`targetField` added to the child's column list).
    de.ii.xtraplatform.features.sql.domain.SqlQueryJoin join = patch.junction.getRelations().get(0);
    String junctionTable = patch.junction.getName();
    String junctionFk = join.getTargetField();
    String parentPk = join.getSourceField();

    String deleteSql =
        "DELETE FROM "
            + junctionTable
            + " WHERE "
            + junctionFk
            + " IN (SELECT "
            + parentPk
            + " FROM "
            + mainTableName
            + " WHERE "
            + idColumnName
            + " = "
            + idLiteral
            + ");";
    sqlSession.runReturning(deleteSql);

    if (patch.values.isEmpty()) {
      return;
    }

    if (patch.objectChildColumns == null) {
      // VALUE_ARRAY: single value column, one literal per element.
      String valueCol = patch.valueColumn.getName();
      StringBuilder valuesList = new StringBuilder();
      for (int i = 0; i < patch.values.size(); i++) {
        if (i > 0) valuesList.append(", ");
        valuesList
            .append("(")
            .append(encodeLiteral(patch.valueColumn, Optional.of(patch.values.get(i)), crs))
            .append(")");
      }
      String insertSql =
          "INSERT INTO "
              + junctionTable
              + " ("
              + junctionFk
              + ", "
              + valueCol
              + ") SELECT m."
              + parentPk
              + ", v.val FROM "
              + mainTableName
              + " m, (VALUES "
              + valuesList
              + ") AS v(val) WHERE m."
              + idColumnName
              + " = "
              + idLiteral
              + ";";
      sqlSession.runReturning(insertSql);
      return;
    }

    // OBJECT_ARRAY: multi-column INSERT per element. Each JSON object value contributes one row
    // with literals for the child columns it sets; unset child columns get SQL NULL.
    List<String> childKeys = new ArrayList<>(patch.objectChildColumns.keySet());
    StringBuilder cols = new StringBuilder(junctionFk);
    for (String childKey : childKeys) {
      cols.append(", ").append(patch.objectChildColumns.get(childKey).getName());
    }
    for (com.fasterxml.jackson.databind.JsonNode element : patch.values) {
      if (!element.isObject()) {
        throw new IllegalArgumentException(
            "Object-array element for property '"
                + patch.objectPath
                + "' must be a JSON object, got: "
                + element.getNodeType());
      }
      StringBuilder selectLits = new StringBuilder("m.").append(parentPk);
      for (String childKey : childKeys) {
        com.fasterxml.jackson.databind.JsonNode v = element.get(childKey);
        selectLits.append(", ");
        selectLits.append(
            encodeLiteral(patch.objectChildColumns.get(childKey), Optional.ofNullable(v), crs));
      }
      String insertSql =
          "INSERT INTO "
              + junctionTable
              + " ("
              + cols
              + ") SELECT "
              + selectLits
              + " FROM "
              + mainTableName
              + " m WHERE m."
              + idColumnName
              + " = "
              + idLiteral
              + ";";
      sqlSession.runReturning(insertSql);
    }
  }

  // Patch state for a junction-backed property. Two modes are encoded in the same record so the
  // executor's per-path map can hold both kinds:
  //   VALUE_ARRAY: `valueColumn` is the single value column; `objectChildColumns == null`.
  //   OBJECT_ARRAY: `valueColumn == null`; `objectChildColumns` maps child schema-ids to their
  //                 columns on the junction (in declaration order so SQL output is deterministic).
  private static final class JunctionPatch {
    final de.ii.xtraplatform.features.sql.domain.SqlQuerySchema junction;
    final de.ii.xtraplatform.features.sql.domain.SqlQueryColumn valueColumn;
    final java.util.LinkedHashMap<String, de.ii.xtraplatform.features.sql.domain.SqlQueryColumn>
        objectChildColumns;
    final String objectPath;
    final List<com.fasterxml.jackson.databind.JsonNode> values = new ArrayList<>();

    private JunctionPatch(
        de.ii.xtraplatform.features.sql.domain.SqlQuerySchema junction,
        de.ii.xtraplatform.features.sql.domain.SqlQueryColumn valueColumn,
        java.util.LinkedHashMap<String, de.ii.xtraplatform.features.sql.domain.SqlQueryColumn>
            objectChildColumns,
        String objectPath) {
      this.junction = junction;
      this.valueColumn = valueColumn;
      this.objectChildColumns = objectChildColumns;
      this.objectPath = objectPath;
    }

    static JunctionPatch valueArray(
        de.ii.xtraplatform.features.sql.domain.SqlQuerySchema junction,
        de.ii.xtraplatform.features.sql.domain.SqlQueryColumn valueColumn) {
      return new JunctionPatch(junction, valueColumn, null, null);
    }

    static JunctionPatch objectArray(
        de.ii.xtraplatform.features.sql.domain.SqlQuerySchema junction,
        FeatureSchema objectSchema,
        SqlQueryMapping mapping,
        String path) {
      java.util.LinkedHashMap<String, de.ii.xtraplatform.features.sql.domain.SqlQueryColumn> cols =
          new java.util.LinkedHashMap<>();
      for (FeatureSchema child : objectSchema.getProperties()) {
        if (child.getType() == de.ii.xtraplatform.features.domain.SchemaBase.Type.OBJECT
            || child.getType() == de.ii.xtraplatform.features.domain.SchemaBase.Type.OBJECT_ARRAY) {
          // Skip nested objects — only flat scalar children are supported in this phase. The
          // caller will see a NULL for those keys, or an error if the user sets them.
          continue;
        }
        String childPath = path + "." + child.getName();
        mapping
            .getColumnForValue(childPath, de.ii.xtraplatform.features.domain.MappingRule.Scope.W)
            .ifPresent(t -> cols.put(child.getName(), t.second()));
      }
      if (cols.isEmpty()) {
        throw new IllegalArgumentException(
            "Object-array property '"
                + path
                + "' has no writable scalar child columns; nested objects are not yet supported"
                + " by partial updates.");
      }
      return new JunctionPatch(junction, null, cols, path);
    }

    void appendValues(Optional<com.fasterxml.jackson.databind.JsonNode> value) {
      if (value.isEmpty() || value.get().isNull()) {
        // Empty value means "clear" — discard any earlier-accumulated values for this path.
        values.clear();
        return;
      }
      com.fasterxml.jackson.databind.JsonNode node = value.get();
      if (node.isArray()) {
        for (com.fasterxml.jackson.databind.JsonNode element : node) {
          if (!element.isNull()) {
            values.add(element);
          }
        }
      } else {
        values.add(node);
      }
    }

    void appendObjectValues(Optional<com.fasterxml.jackson.databind.JsonNode> value) {
      if (value.isEmpty() || value.get().isNull()) {
        values.clear();
        return;
      }
      com.fasterxml.jackson.databind.JsonNode node = value.get();
      if (node.isArray()) {
        for (com.fasterxml.jackson.databind.JsonNode element : node) {
          if (!element.isNull()) {
            values.add(element);
          }
        }
      } else if (node.isObject()) {
        values.add(node);
      } else {
        throw new IllegalArgumentException(
            "Object-array property '"
                + objectPath
                + "' requires a JSON object (or array of objects) as the value, got: "
                + node.getNodeType());
      }
    }
  }

  // SQL literal for a typed column value. Scalar/datetime/boolean become a quoted/raw literal.
  // Geometry columns (WKT/WKB operations) decode the JsonNode as a GeoJSON geometry, apply CRS
  // transform from the request CRS to the provider's native CRS, and emit
  // `ST_GeomFromText('<wkt>', <native_srid>)` (with `ST_ForcePolygonCW` for polygons, matching
  // the encoding the INSERT path uses in FeatureEncoderSql.toWkt).
  private String encodeLiteral(
      de.ii.xtraplatform.features.sql.domain.SqlQueryColumn column,
      Optional<com.fasterxml.jackson.databind.JsonNode> valueOpt,
      EpsgCrs crs) {
    if (valueOpt.isEmpty() || valueOpt.get().isNull()) {
      return "NULL";
    }
    com.fasterxml.jackson.databind.JsonNode value = valueOpt.get();
    if (column.hasOperation(de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation.WKT)
        || column.hasOperation(
            de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation.WKB)) {
      return encodeGeometryLiteral(column, value, crs);
    }
    de.ii.xtraplatform.features.domain.SchemaBase.Type type = column.getType();
    if (type == de.ii.xtraplatform.features.domain.SchemaBase.Type.STRING
        || type == de.ii.xtraplatform.features.domain.SchemaBase.Type.DATETIME
        || type == de.ii.xtraplatform.features.domain.SchemaBase.Type.DATE) {
      return sqlString(value.asText());
    }
    if (type == de.ii.xtraplatform.features.domain.SchemaBase.Type.BOOLEAN) {
      return value.asBoolean() ? "TRUE" : "FALSE";
    }
    if (type == de.ii.xtraplatform.features.domain.SchemaBase.Type.INTEGER
        || type == de.ii.xtraplatform.features.domain.SchemaBase.Type.FLOAT) {
      return value.asText();
    }
    // Fallback — treat as string. Avoids generating invalid SQL on niche column types we haven't
    // wired up explicitly yet (FEATURE_REF, etc.).
    return sqlString(value.asText());
  }

  private String encodeGeometryLiteral(
      de.ii.xtraplatform.features.sql.domain.SqlQueryColumn column,
      com.fasterxml.jackson.databind.JsonNode value,
      EpsgCrs crs) {
    if (!value.isObject()) {
      throw new IllegalArgumentException(
          "Geometry property '"
              + column.getName()
              + "' requires a GeoJSON geometry object as the value, got: "
              + value.getNodeType());
    }
    de.ii.xtraplatform.geometries.domain.Geometry<?> geometry;
    try {
      geometry =
          new de.ii.xtraplatform.geometries.domain.transcode.json.GeometryDecoderJson(true)
              .decode(value, Optional.ofNullable(crs), Optional.empty());
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Could not parse GeoJSON geometry for property '"
              + column.getName()
              + "': "
              + e.getMessage(),
          e);
    }
    if (crs != null && !Objects.equals(crs, nativeCrs)) {
      Optional<de.ii.xtraplatform.crs.domain.CrsTransformer> transformer =
          crsTransformerFactory.getTransformer(crs, nativeCrs);
      if (transformer.isPresent()) {
        geometry =
            geometry.accept(
                new de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformer(
                    de.ii.xtraplatform.geometries.domain.transform.ImmutableCrsTransform.of(
                        Optional.empty(), transformer.get())));
      }
    }
    String wkt;
    try {
      wkt =
          new de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt()
              .encode(geometry);
    } catch (java.io.IOException e) {
      throw new IllegalStateException(
          "Could not encode geometry as WKT for property '"
              + column.getName()
              + "': "
              + e.getMessage(),
          e);
    }
    String result = String.format("ST_GeomFromText('%s',%s)", wkt, nativeCrs.getCode());
    if (geometry.getType() == de.ii.xtraplatform.geometries.domain.GeometryType.POLYGON
        || geometry.getType() == de.ii.xtraplatform.geometries.domain.GeometryType.MULTI_POLYGON) {
      result = String.format("ST_ForcePolygonCW(%s)", result);
    }
    return result;
  }

  // Walk a canonical schema-id path (e.g. ["mat"], or ["lzi","beg"]) from the feature's main
  // schema. Returns the last matched FeatureSchema, or null if any segment doesn't resolve.
  private static FeatureSchema resolveSchemaByPath(FeatureSchema root, List<String> path) {
    if (root == null || path == null || path.isEmpty()) {
      return null;
    }
    FeatureSchema cursor = root;
    for (String segment : path) {
      FeatureSchema next = null;
      for (FeatureSchema child : cursor.getProperties()) {
        if (Objects.equals(child.getName(), segment)) {
          next = child;
          break;
        }
      }
      if (next == null) {
        return null;
      }
      cursor = next;
    }
    return cursor;
  }

  private static String sqlString(String value) {
    if (value == null) {
      return "NULL";
    }
    return "'" + value.replace("'", "''") + "'";
  }

  @Override
  public void commit() {
    sqlSession.commit();
  }

  @Override
  public void rollback() {
    sqlSession.rollback();
  }

  @Override
  public void close() {
    sqlSession.close();
  }

  private FeatureTransactions.MutationResult writeFeatures(
      FeatureTransactions.MutationResult.Type type,
      String featureType,
      FeatureTokenSource featureTokenSource,
      Optional<String> featureId,
      EpsgCrs crs,
      boolean partial) {
    SqlQueryMapping mapping = requireMapping(featureType);

    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder().type(type).hasFeatures(false);

    List<FeatureDataSql> collected = new ArrayList<>();
    try {
      drainSource(featureTokenSource, mapping, crs, builder, collected, partial);
    } catch (RuntimeException e) {
      return builder.error(translateEncoderError(e)).build();
    }

    RowCursor rowCursor = new RowCursor(mapping.getMainTable().getFullPath());
    boolean deleteFirst =
        type == FeatureTransactions.MutationResult.Type.UPDATE
            || type == FeatureTransactions.MutationResult.Type.REPLACE;

    // Role-id column on the main table — its value in the inserted feature is the externally
    // visible feature id (e.g. ALKIS gml:id stored in 'objid'); fall back to the surrogate PK only
    // when no role-id column / no value is present.
    Optional<
            de.ii.xtraplatform.base.domain.util.Tuple<
                de.ii.xtraplatform.features.sql.domain.SqlQuerySchema,
                de.ii.xtraplatform.features.sql.domain.SqlQueryColumn>>
        roleIdColumn = mapping.getColumnForId();
    String roleIdColumnName = roleIdColumn.map(t -> t.second().getName()).orElse(null);
    de.ii.xtraplatform.features.sql.domain.SqlQuerySchema roleIdTable =
        roleIdColumn.map(de.ii.xtraplatform.base.domain.util.Tuple::first).orElse(null);

    try {
      if (deleteFirst) {
        writeFeaturesPerFeature(
            collected, rowCursor, featureId, crs, true, roleIdTable, roleIdColumnName, builder);
      } else {
        writeFeaturesBatched(
            collected, rowCursor, featureId, crs, roleIdTable, roleIdColumnName, builder);
      }
    } catch (RuntimeException e) {
      builder.error(e);
    }

    return builder.build();
  }

  // Assembles the token-source → FeatureEncoderSql pipeline and drains it into `collected`. A
  // fresh FeatureTokenStatsCollector is instantiated per source so internal transformer state
  // doesn't leak across runs; the result builder is shared, so accumulated stats (bbox, temporal
  // extent) overwrite per source — matching the existing single-source-multi-feature semantics
  // where the last feature's stats win.
  private void drainSource(
      FeatureTokenSource featureTokenSource,
      SqlQueryMapping mapping,
      EpsgCrs crs,
      ImmutableMutationResult.Builder builder,
      List<FeatureDataSql> collected,
      boolean partial) {
    FeatureTokenStatsCollector statsCollector = new FeatureTokenStatsCollector(builder, crs);

    Source<FeatureDataSql> featureSqlSource =
        featureTokenSource
            .via(statsCollector)
            .via(
                new FeatureEncoderSql(
                    mapping,
                    crs,
                    nativeCrs,
                    crsTransformerFactory,
                    nativeTimeZone,
                    partial ? Optional.of(FeatureTransactions.PATCH_NULL_VALUE) : Optional.empty()))
            .via(Transformer.map(feature -> (FeatureDataSql) feature));

    if (partial) {
      featureSqlSource =
          featureSqlSource.via(
              Transformer.reduce(
                  ModifiableFeatureDataSql.create(),
                  (a, b) -> a.getRows().isEmpty() ? b : a.patchWith(b)));
    }

    featureSqlSource
        .to(Sink.foreach(collected::add))
        .on(streamRunner)
        .run()
        .toCompletableFuture()
        .join();
  }

  // Original per-feature loop, kept for the UPDATE/REPLACE path (each feature is preceded by a
  // DELETE so its statements can't be merged with another feature's at the SQL level).
  private void writeFeaturesPerFeature(
      List<FeatureDataSql> collected,
      RowCursor rowCursor,
      Optional<String> featureId,
      EpsgCrs crs,
      boolean deleteFirst,
      de.ii.xtraplatform.features.sql.domain.SqlQuerySchema roleIdTable,
      String roleIdColumnName,
      ImmutableMutationResult.Builder builder) {
    for (FeatureDataSql feature : collected) {
      // When the role-id value is already in the feature row (e.g. ALKIS gml:id parsed into
      // 'objid'), don't pass it to createInstanceInserts — that path injects/quotes it again as
      // the id column value, producing duplicate-quoted SQL. We still pass it to sqlSession.run
      // so the returned id is the role-id rather than the surrogate PK.
      Optional<String> roleIdFromRow = extractRoleId(feature, roleIdTable, roleIdColumnName);
      Optional<String> effectiveFeatureId = featureId.or(() -> roleIdFromRow);
      List<Supplier<Tuple<String, Consumer<String>>>> tuples =
          featureMutationsSql.createInstanceInserts(
              feature, rowCursor, featureId, crs, deleteFirst);
      List<Supplier<String>> statements =
          tuples.stream()
              .map(
                  t ->
                      (Supplier<String>)
                          () -> {
                            Tuple<String, Consumer<String>> v = t.get();
                            return v.first();
                          })
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      List<Consumer<String>> idConsumers =
          tuples.stream().map(t -> t.get().second()).collect(Collectors.toList());
      String id = sqlSession.run(statements, idConsumers, effectiveFeatureId);
      if (id != null) {
        builder.addIds(id);
      }
    }
  }

  // CREATE path: materialize every feature's statement list upfront, fold consecutive main
  // INSERTs that share the same "INSERT INTO t (...) VALUES " prefix and " RETURNING <pk>;" suffix
  // into one multi-row INSERT per group, distribute the returned PKs back to each feature's
  // consumer (so child statements can read them from currentRow.ids), then send all children
  // across all features through a single sqlSession.run — letting the existing JDBC batch path
  // collapse the entire tail into one executeBatch.
  private void writeFeaturesBatched(
      List<FeatureDataSql> collected,
      RowCursor rowCursor,
      Optional<String> featureId,
      EpsgCrs crs,
      de.ii.xtraplatform.features.sql.domain.SqlQuerySchema roleIdTable,
      String roleIdColumnName,
      ImmutableMutationResult.Builder builder) {
    int n = collected.size();
    List<Optional<String>> effectiveIds = new ArrayList<>(n);
    String[] mainSqls = new String[n];
    Consumer<?>[] mainConsumersRaw = new Consumer<?>[n];
    List<List<Supplier<Tuple<String, Consumer<String>>>>> childTuples = new ArrayList<>(n);

    for (int i = 0; i < n; i++) {
      FeatureDataSql feature = collected.get(i);
      Optional<String> roleIdFromRow = extractRoleId(feature, roleIdTable, roleIdColumnName);
      effectiveIds.add(featureId.or(() -> roleIdFromRow));

      List<Supplier<Tuple<String, Consumer<String>>>> tuples =
          featureMutationsSql.createInstanceInserts(feature, rowCursor, featureId, crs, false);

      Tuple<String, Consumer<String>> main = tuples.isEmpty() ? null : tuples.get(0).get();
      mainSqls[i] = main == null ? null : main.first();
      mainConsumersRaw[i] = main == null ? null : main.second();
      childTuples.add(tuples.isEmpty() ? List.of() : tuples.subList(1, tuples.size()));
    }

    String[] returnedPks = new String[n];

    runMainInsertsGrouped(mainSqls, mainConsumersRaw, returnedPks);

    // Publish the per-feature ids as soon as the main inserts have returned; if the children
    // phase then fails, the result still carries every id that made it into the transaction
    // (the surrounding executor decides whether to roll back).
    for (int i = 0; i < n; i++) {
      String id = effectiveIds.get(i).orElse(returnedPks[i]);
      if (id != null) {
        builder.addIds(id);
      }
    }

    runChildren(childTuples);
  }

  // Groups consecutive features whose main-insert SQL shares an "INSERT INTO t (...) VALUES "
  // prefix and a " RETURNING <pk>;" suffix into one combined multi-row INSERT, executed via
  // sqlSession.runReturning so we receive every generated PK in insertion order. Features whose
  // main SQL doesn't fit the shape (e.g. DEFAULT VALUES, or a different table) are flushed as a
  // single-row insert via the same path; null-SQL slots are skipped entirely.
  private void runMainInsertsGrouped(
      String[] mainSqls, Consumer<?>[] consumersRaw, String[] returnedPks) {
    int n = mainSqls.length;
    int i = 0;
    while (i < n) {
      if (mainSqls[i] == null) {
        i++;
        continue;
      }
      MainInsertParts head = splitMainInsert(mainSqls[i]);
      if (head == null) {
        // SQL doesn't match the standard shape — execute on its own.
        executeAndDispatch(mainSqls[i], consumersRaw, returnedPks, new int[] {i});
        i++;
        continue;
      }
      List<Integer> groupIdx = new ArrayList<>();
      List<String> valuesTuples = new ArrayList<>();
      groupIdx.add(i);
      valuesTuples.add(head.values);
      int j = i + 1;
      while (j < n) {
        if (mainSqls[j] == null) {
          break;
        }
        MainInsertParts next = splitMainInsert(mainSqls[j]);
        if (next == null || !head.prefix.equals(next.prefix) || !head.suffix.equals(next.suffix)) {
          break;
        }
        groupIdx.add(j);
        valuesTuples.add(next.values);
        j++;
      }
      String combined = head.prefix + String.join(",", valuesTuples) + head.suffix;
      int[] indices = groupIdx.stream().mapToInt(Integer::intValue).toArray();
      executeAndDispatch(combined, consumersRaw, returnedPks, indices);
      i = j;
    }
  }

  @SuppressWarnings("unchecked")
  private void executeAndDispatch(
      String sql, Consumer<?>[] consumersRaw, String[] returnedPks, int[] featureIndices) {
    List<String> ids = sqlSession.runReturning(sql);
    for (int k = 0; k < featureIndices.length; k++) {
      String returned = k < ids.size() ? ids.get(k) : null;
      int featureIdx = featureIndices[k];
      returnedPks[featureIdx] = returned;
      Consumer<String> c = (Consumer<String>) consumersRaw[featureIdx];
      if (c != null) {
        c.accept(returned);
      }
    }
  }

  private void runChildren(List<List<Supplier<Tuple<String, Consumer<String>>>>> childTuples) {
    List<Supplier<String>> statements = new ArrayList<>();
    List<Consumer<String>> idConsumers = new ArrayList<>();
    for (List<Supplier<Tuple<String, Consumer<String>>>> tuples : childTuples) {
      for (Supplier<Tuple<String, Consumer<String>>> t : tuples) {
        statements.add(
            () -> {
              Tuple<String, Consumer<String>> v = t.get();
              return v.first();
            });
        idConsumers.add(t.get().second());
      }
    }
    if (statements.isEmpty()) {
      return;
    }
    sqlSession.run(statements, idConsumers, Optional.empty());
  }

  // Splits "INSERT INTO t (...) VALUES (...) RETURNING <pk>;" into prefix (ending with "VALUES "),
  // values tuple (the parenthesised values, including the outer parens), and suffix (starting with
  // " RETURNING "). Returns null when the SQL doesn't match — e.g. DEFAULT VALUES, or some other
  // shape — so the caller can fall back to a single-row execution.
  private static MainInsertParts splitMainInsert(String sql) {
    int retIdx = sql.lastIndexOf(" RETURNING ");
    if (retIdx < 0 || !sql.endsWith(";")) {
      return null;
    }
    String suffix = sql.substring(retIdx);
    String body = sql.substring(0, retIdx);
    int valIdx = body.lastIndexOf(" VALUES (");
    if (valIdx < 0 || !body.endsWith(")")) {
      return null;
    }
    int prefixEnd = valIdx + " VALUES ".length();
    String prefix = sql.substring(0, prefixEnd);
    String values = body.substring(prefixEnd);
    return new MainInsertParts(prefix, values, suffix);
  }

  private static final class MainInsertParts {
    final String prefix;
    final String values;
    final String suffix;

    MainInsertParts(String prefix, String values, String suffix) {
      this.prefix = prefix;
      this.values = values;
      this.suffix = suffix;
    }
  }

  private static Optional<String> extractRoleId(
      FeatureDataSql feature,
      de.ii.xtraplatform.features.sql.domain.SqlQuerySchema roleIdTable,
      String roleIdColumnName) {
    if (roleIdTable == null || roleIdColumnName == null) {
      return Optional.empty();
    }
    return feature.getRows().stream()
        .filter(r -> Objects.equals(r.first(), roleIdTable))
        .findFirst()
        .map(r -> r.second().getValues().get(roleIdColumnName))
        .map(SqlMutationSession::unquoteSqlLiteral);
  }

  /**
   * Strips the SQL string-literal wrapping that {@link FeatureDataSql} stores in row values (text
   * values land in the map as {@code 'foo''bar'}, numerics/booleans as-is). Returns the inner value
   * with doubled single quotes collapsed; non-quoted inputs are returned unchanged.
   */
  private static String unquoteSqlLiteral(String value) {
    if (value == null || value.length() < 2) {
      return value;
    }
    if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
      return value.substring(1, value.length() - 1).replace("''", "'");
    }
    return value;
  }

  private SqlQueryMapping requireMapping(String featureType) {
    List<SqlQueryMapping> mappings = queryMappings.get(featureType);
    if (mappings == null || mappings.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Feature type '%s' not found.", featureType));
    }
    return mappings.get(0);
  }

  private Throwable translateEncoderError(Throwable throwable) {
    Throwable cause = throwable instanceof RuntimeException ? throwable.getCause() : throwable;
    if (cause instanceof PSQLException || cause instanceof JsonParseException) {
      LogContext.errorAsDebug(LOGGER, cause, "Error during feature mutation");
      return new IllegalArgumentException(
          "Invalid feature data. You may be able to obtain more information about the problem by adding the header 'Prefer: handling=strict' to the request.",
          cause);
    }
    return throwable;
  }
}
