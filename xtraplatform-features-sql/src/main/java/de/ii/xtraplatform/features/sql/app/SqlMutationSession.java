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
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenSource;
import de.ii.xtraplatform.features.domain.FeatureTransactions;
import de.ii.xtraplatform.features.domain.ImmutableMutationResult;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.FeatureTokenStatsCollector;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.SqlSession;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.transcode.json.GeometryDecoderJson;
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt;
import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformer;
import de.ii.xtraplatform.geometries.domain.transform.ImmutableCrsTransform;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.streams.domain.Reactive.Source;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import java.time.Instant;
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
    return createFeatures(featureType, featureTokenSources, crs, Map.of());
  }

  // Same as the 3-arg overload, but after every source is drained the per-role column overrides
  // are applied row-by-row to the collected FeatureDataSql instances: an entry whose value is
  // non-null forces the role-bearing column to that value (already SQL-literal-formatted, e.g.
  // quoted for DATETIME); a null value clears the column so it lands as SQL NULL. Roles that the
  // type's schema mapping does not bind to a column are silently ignored — the override map is
  // a hint, not a requirement.
  @Override
  public FeatureTransactions.MutationResult createFeatures(
      String featureType,
      Iterable<FeatureTokenSource> featureTokenSources,
      EpsgCrs crs,
      Map<SchemaBase.Role, Object> roleOverrides) {
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

    if (!roleOverrides.isEmpty()) {
      for (FeatureDataSql feature : collected) {
        applyRoleOverrides(feature, roleOverrides);
      }
    }

    RowCursor rowCursor = new RowCursor(mapping.getMainTable().getFullPath());
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        roleIdColumn = mapping.getColumnForId();
    String roleIdColumnName = roleIdColumn.map(t -> t.second().getName()).orElse(null);
    SqlQuerySchema roleIdTable =
        roleIdColumn.map(de.ii.xtraplatform.base.domain.util.Tuple::first).orElse(null);

    try {
      writeFeaturesBatched(
          collected, rowCursor, Optional.empty(), crs, roleIdTable, roleIdColumnName, builder);
    } catch (RuntimeException e) {
      builder.error(e);
    }

    FeatureTransactions.MutationResult result = builder.build();
    if (!roleOverrides.isEmpty() && result.getError().isEmpty()) {
      applyPostInsertRoleOverrides(mapping, result.getIds(), roleOverrides);
    }
    return result;
  }

  // Role overrides whose target column is in the read scope but NOT in the writable scope
  // (e.g. PREDECESSOR_INTERVAL_START on a versioned collection whose denorm property is
  // configured `excludedScopes: [RECEIVABLE, SORTABLE]` → MappingRule.Scope.RC) are silently
  // dropped by the INSERT generator because it only emits columns in `getWritableColumns()`.
  // Mirror the retire-side hand-built UPDATE: for every just-inserted row, emit
  // `UPDATE main SET <col> = <lit>[, …] WHERE <id_col> = '<id>' AND <end_col> IS NULL`, where
  // the end-col predicate narrows to the just-inserted open version (versioned collections
  // bind `PRIMARY_INTERVAL_END`; plain collections skip the predicate). Role overrides whose
  // column IS writable have already landed via the INSERT path and are skipped here.
  private void applyPostInsertRoleOverrides(
      SqlQueryMapping mapping, List<String> ids, Map<SchemaBase.Role, Object> overrides) {
    if (ids.isEmpty()) {
      return;
    }
    List<de.ii.xtraplatform.base.domain.util.Tuple<SqlQueryColumn, Object>> deferred =
        new ArrayList<>();
    SqlQuerySchema postUpdateTable = null;
    for (Map.Entry<SchemaBase.Role, Object> entry : overrides.entrySet()) {
      Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> resolved =
          mapping.getColumnForRole(entry.getKey());
      if (resolved.isEmpty()) {
        continue;
      }
      SqlQuerySchema table = resolved.get().first();
      SqlQueryColumn column = resolved.get().second();
      boolean writable =
          table.getWritableColumns().stream().anyMatch(c -> c.getName().equals(column.getName()));
      if (writable) {
        continue;
      }
      if (postUpdateTable == null) {
        postUpdateTable = table;
      } else if (postUpdateTable != table) {
        // All deferred overrides for a single feature must target the same table (typically the
        // main table). A future role binding on a sub-table would need a separate UPDATE.
        continue;
      }
      deferred.add(de.ii.xtraplatform.base.domain.util.Tuple.of(column, entry.getValue()));
    }
    if (deferred.isEmpty() || postUpdateTable == null) {
      return;
    }
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> idColumn =
        mapping.getColumnForId();
    if (idColumn.isEmpty()) {
      return;
    }
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> endColumn =
        mapping.getColumnForPrimaryIntervalEnd();
    StringBuilder setClause = new StringBuilder();
    for (de.ii.xtraplatform.base.domain.util.Tuple<SqlQueryColumn, Object> d : deferred) {
      if (setClause.length() > 0) {
        setClause.append(", ");
      }
      setClause
          .append(d.first().getName())
          .append(" = ")
          .append(d.second() == null ? "NULL" : formatRoleOverrideValue(d.first(), d.second()));
    }
    String tableName = postUpdateTable.getName();
    String idColName = idColumn.get().second().getName();
    String endPredicate =
        endColumn.map(t -> " AND " + t.second().getName() + " IS NULL").orElse("");
    for (String id : ids) {
      String sql =
          "UPDATE "
              + tableName
              + " SET "
              + setClause
              + " WHERE "
              + idColName
              + " = "
              + sqlString(id)
              + endPredicate
              + ";";
      sqlSession.runReturning(sql);
    }
  }

  // For each role in `overrides`, look up the (table, column) on the mapping and either overwrite
  // (non-null value) or clear (null value) the entry in the matching row's `values` map. Values
  // are stored in the same SQL-literal form the encoder uses (single-quoted strings/datetimes,
  // bare numerics), so the caller is expected to pre-format. Roles whose column the mapping does
  // not resolve are ignored.
  private static void applyRoleOverrides(
      FeatureDataSql feature, Map<SchemaBase.Role, Object> overrides) {
    SqlQueryMapping mapping = feature.getMapping();
    for (Map.Entry<SchemaBase.Role, Object> entry : overrides.entrySet()) {
      Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> resolved =
          mapping.getColumnForRole(entry.getKey());
      if (resolved.isEmpty()) {
        continue;
      }
      SqlQuerySchema table = resolved.get().first();
      SqlQueryColumn column = resolved.get().second();
      for (de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, ModifiableSqlRowData> row :
          feature.getRows()) {
        if (Objects.equals(row.first(), table)) {
          if (entry.getValue() == null) {
            row.second().getValues().remove(column.getName());
          } else {
            row.second()
                .putValues(column.getName(), formatRoleOverrideValue(column, entry.getValue()));
          }
          break;
        }
      }
    }
  }

  private static String formatRoleOverrideValue(SqlQueryColumn column, Object value) {
    SchemaBase.Type type = column.getType();
    String raw = value.toString();
    if (type == SchemaBase.Type.STRING
        || type == SchemaBase.Type.DATETIME
        || type == SchemaBase.Type.DATE) {
      return "'" + raw.replace("'", "''") + "'";
    }
    return raw;
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

  // Versioned retirement: closes the open version of `featureId` by setting its
  // PRIMARY_INTERVAL_END column to `retirementTimestamp`, gated by `PRIMARY_INTERVAL_END IS NULL`
  // so a concurrent retirement loses without corrupting the timeline. Returns the role-id of the
  // retired row; an empty result means no open version matched and is mapped by the caller to a
  // 409-style conflict. When `expectedStart` is present, the WHERE also requires `startCol =
  // expectedStart` — an If-Unmodified-Since-style check that the caller maps to a 412 on miss.
  @Override
  public FeatureTransactions.MutationResult retireFeature(
      String featureType,
      String featureId,
      Instant retirementTimestamp,
      Optional<Instant> expectedStart) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.UPDATE)
            .hasFeatures(false);

    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> endColumn =
        mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_END);
    if (endColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' has no PRIMARY_INTERVAL_END role column; cannot retire."))
          .build();
    }
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        startColumn = mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_START);
    if (startColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' has no PRIMARY_INTERVAL_START role column; cannot enforce"
                      + " no-backdating during retire."))
          .build();
    }
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> idColumn =
        mapping.getColumnForId();
    if (idColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '" + featureType + "' has no id column; cannot retire."))
          .build();
    }

    SqlQuerySchema endTable = endColumn.get().first();
    SqlQuerySchema idTable = idColumn.get().first();
    SqlQuerySchema startTable = startColumn.get().first();
    if (!Objects.equals(endTable.getName(), idTable.getName())
        || !Objects.equals(startTable.getName(), idTable.getName())) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' has id / PRIMARY_INTERVAL_START / PRIMARY_INTERVAL_END on more"
                      + " than one table; retirement requires all three on the main table."))
          .build();
    }

    String mainTableName = endTable.getName();
    String endColumnName = endColumn.get().second().getName();
    String startColumnName = startColumn.get().second().getName();
    String idColumnName = idColumn.get().second().getName();
    String tsLiteral = sqlString(retirementTimestamp.toString());

    // Denorm SUCCESSOR_INTERVAL_START (plan §1.6, option (i)): if the schema mapping binds the
    // role to a column on the main table, set it to the retirement timestamp — which is also
    // the new version's start in retire-and-insert flows. Opt-in: no SUCCESSOR_INTERVAL_START
    // role on the schema means no SET clause is added.
    StringBuilder setClause = new StringBuilder(endColumnName).append(" = ").append(tsLiteral);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        successorColumn = mapping.getColumnForRole(SchemaBase.Role.SUCCESSOR_INTERVAL_START);
    if (successorColumn.isPresent()
        && Objects.equals(successorColumn.get().first().getName(), mainTableName)) {
      setClause
          .append(", ")
          .append(successorColumn.get().second().getName())
          .append(" = ")
          .append(tsLiteral);
    }

    // Optimistic-concurrency + no-backdating in one atomic UPDATE: the row must be the open
    // version (endCol IS NULL) AND its start must be strictly before the retirement timestamp
    // (no-backdating, plan §1.5). A backdated or non-existent retire matches 0 rows; the caller
    // surfaces that as a 409. When `expectedStart` is present, an additional `startCol =
    // expectedStart` predicate is appended — an If-Unmodified-Since-style check that maps to a
    // 412 on miss (plan §1.8 composite-id convention).
    StringBuilder where =
        new StringBuilder(idColumnName)
            .append(" = ")
            .append(sqlString(featureId))
            .append(" AND ")
            .append(endColumnName)
            .append(" IS NULL AND ")
            .append(startColumnName)
            .append(" < ")
            .append(tsLiteral);
    if (expectedStart.isPresent()) {
      where
          .append(" AND ")
          .append(startColumnName)
          .append(" = ")
          .append(sqlString(expectedStart.get().toString()));
    }
    String sql =
        "UPDATE "
            + mainTableName
            + " SET "
            + setClause
            + " WHERE "
            + where
            + " RETURNING "
            + idColumnName
            + ";";
    try {
      List<String> returned = sqlSession.runReturning(sql);
      for (String id : returned) {
        builder.addIds(id);
      }
    } catch (RuntimeException e) {
      builder.error(e);
    }
    return builder.build();
  }

  // Versioned-Insert pre-flight (plan §1.5 Part A.insert): refuses to write a new feature row when
  // any version of the same role-id already exists (open or retired). Clients add new versions of
  // an existing feature through Replace / Update / Delete; Insert is reserved for brand-new ids.
  // The check runs as a single SELECT on the main table and returns an error result the caller
  // maps to a 409.
  @Override
  public FeatureTransactions.MutationResult assertNoConflictingVersion(
      String featureType, String featureId) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.CREATE)
            .hasFeatures(false);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> idColumn =
        mapping.getColumnForId();
    if (idColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' has no id column; cannot run the versioned-insert pre-flight."))
          .build();
    }
    String mainTableName = idColumn.get().first().getName();
    String idCol = idColumn.get().second().getName();
    String sql =
        "SELECT 1 FROM "
            + mainTableName
            + " WHERE "
            + idCol
            + " = "
            + sqlString(featureId)
            + " LIMIT 1;";
    try {
      List<String> hit = sqlSession.runReturning(sql);
      if (!hit.isEmpty()) {
        return builder
            .error(
                new IllegalArgumentException(
                    "Cannot create feature id '"
                        + featureId
                        + "' in collection '"
                        + featureType
                        + "': a version of this feature already exists (use Replace or Update to"
                        + " add a new version)."))
            .build();
      }
    } catch (RuntimeException e) {
      builder.error(e);
    }
    return builder.build();
  }

  // Reads the open version's PRIMARY_INTERVAL_START value for `featureId`. Used by versioned
  // retire-and-insert flows (plan §1.6) to populate the new row's PREDECESSOR_INTERVAL_START
  // denorm column. Returns empty when no open version exists or the type lacks the required
  // columns — the caller treats the value as "no predecessor info available" and omits the
  // override.
  @Override
  public Optional<String> getOpenVersionStart(String featureType, String featureId) {
    SqlQueryMapping mapping = requireMapping(featureType);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> idColumn =
        mapping.getColumnForId();
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        startColumn = mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_START);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> endColumn =
        mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_END);
    if (idColumn.isEmpty() || startColumn.isEmpty() || endColumn.isEmpty()) {
      return Optional.empty();
    }
    String mainTableName = idColumn.get().first().getName();
    if (!Objects.equals(startColumn.get().first().getName(), mainTableName)
        || !Objects.equals(endColumn.get().first().getName(), mainTableName)) {
      return Optional.empty();
    }
    String sql =
        "SELECT "
            + startColumn.get().second().getName()
            + " FROM "
            + mainTableName
            + " WHERE "
            + idColumn.get().second().getName()
            + " = "
            + sqlString(featureId)
            + " AND "
            + endColumn.get().second().getName()
            + " IS NULL LIMIT 1;";
    try {
      List<String> rows = sqlSession.runReturning(sql);
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      return Optional.ofNullable(rows.get(0));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
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
    return patchInternal(featureType, featureId, updates, crs, "", "feature");
  }

  // Same as patchFeature but additionally constrains the target row to the open version (the row
  // whose PRIMARY_INTERVAL_END column is currently NULL). The same predicate is propagated into
  // every junction patch's subquery so junction rows are only touched on the open parent.
  //
  // No-backdating (plan §1.5): when one of the `updates` sets the PRIMARY_INTERVAL_END column to
  // a value V, also require `startCol < V` in the WHERE so a retire-in-place Update that would
  // produce a zero-or-negative interval matches 0 rows and surfaces as a 409. We scan the
  // updates upfront, find the end-setting one, format the value as the same SQL literal the
  // patch would have written, and inject it into the extra predicate.
  @Override
  public FeatureTransactions.MutationResult patchOpenVersion(
      String featureType,
      String featureId,
      List<FeatureTransactions.PropertyUpdate> updates,
      EpsgCrs crs,
      Optional<Instant> expectedStart) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.UPDATE)
            .hasFeatures(false);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> endColumn =
        mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_END);
    if (endColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' has no PRIMARY_INTERVAL_END role column; cannot patch open version."))
          .build();
    }
    String endColumnName = endColumn.get().second().getName();
    String extra = " AND " + endColumnName + " IS NULL";

    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        startColumn = mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_START);
    if (startColumn.isPresent()) {
      String startColumnName = startColumn.get().second().getName();
      for (FeatureTransactions.PropertyUpdate u : updates) {
        if (u.getValue().isEmpty()) {
          continue;
        }
        String joined = String.join(".", u.getPath());
        Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
            resolved = mapping.getColumnForValue(joined, MappingRule.Scope.W);
        if (resolved.isPresent()
            && Objects.equals(resolved.get().second().getName(), endColumnName)) {
          String endLiteral = encodeLiteral(resolved.get().second(), u.getValue(), crs);
          extra = extra + " AND " + startColumnName + " < " + endLiteral;
          break;
        }
      }
      // Composite-id If-Unmodified-Since predicate (plan §1.8): the open version's start must
      // equal the value the client encoded in the rid's suffix. Otherwise the UPDATE matches 0
      // rows and the caller maps that to a 412 Precondition Failed.
      if (expectedStart.isPresent()) {
        extra =
            extra + " AND " + startColumnName + " = " + sqlString(expectedStart.get().toString());
      }
    }

    return patchInternal(featureType, featureId, updates, crs, extra, "open version of feature");
  }

  // Versioned Update CLONE_AND_PATCH (plan §1.3): create a new version of the open row, carry
  // forward every column, apply the property updates, and retire the previous open version. The
  // sequence is:
  //   1. SELECT the open row's surrogate PK ([+ start when the predecessor role is bound]).
  //   2. INSERT INTO main (cols) SELECT … FROM main m WHERE m.pk = OLD_PK, with role-driven
  //      overrides (start=ts, end=NULL, predecessor=OLD_start, successor=NULL); RETURNING NEW_PK.
  //   3. For each writable junction table, INSERT … SELECT carrying every column forward except
  //      the FK, which is redirected to NEW_PK.
  //   4. Retire OLD: UPDATE main SET endCol=ts [, successorCol=ts] WHERE pk=OLD_PK AND endCol IS
  //      NULL AND startCol < ts. (Same guards as retireFeature.)
  //   5. Apply property patches to the now-only open row via patchInternal — main-table scalar
  //      patches land as a single UPDATE; VALUE_ARRAY / OBJECT_ARRAY patches reuse the existing
  //      DELETE+INSERT junction path.
  // An empty result on step 1 → caller maps to 409 (or 412 when `expectedStart` was present).
  @Override
  public FeatureTransactions.MutationResult cloneAndPatchFeature(
      String featureType,
      String featureId,
      List<FeatureTransactions.PropertyUpdate> updates,
      Instant mutationTimestamp,
      EpsgCrs crs,
      Optional<Instant> expectedStart) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.UPDATE)
            .hasFeatures(false);

    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> idColumn =
        mapping.getColumnForId();
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        startColumn = mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_START);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> endColumn =
        mapping.getColumnForRole(SchemaBase.Role.PRIMARY_INTERVAL_END);
    if (idColumn.isEmpty() || startColumn.isEmpty() || endColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' is missing ID / PRIMARY_INTERVAL_START / PRIMARY_INTERVAL_END role"
                      + " columns; cannot clone-and-patch."))
          .build();
    }
    SqlQuerySchema mainTable = idColumn.get().first();
    String mainTableName = mainTable.getName();
    if (!Objects.equals(startColumn.get().first().getName(), mainTableName)
        || !Objects.equals(endColumn.get().first().getName(), mainTableName)) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '"
                      + featureType
                      + "' has id / PRIMARY_INTERVAL_START / PRIMARY_INTERVAL_END on more than"
                      + " one table; clone-and-patch requires all three on the main table."))
          .build();
    }
    String idColumnName = idColumn.get().second().getName();
    String startColumnName = startColumn.get().second().getName();
    String endColumnName = endColumn.get().second().getName();
    String pkColumnName = mainTable.getSortKey();
    String tsLiteral = sqlString(mutationTimestamp.toString());

    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        predecessorColumn = mapping.getColumnForRole(SchemaBase.Role.PREDECESSOR_INTERVAL_START);
    boolean predecessorOnMain =
        predecessorColumn.isPresent()
            && Objects.equals(predecessorColumn.get().first().getName(), mainTableName);
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        successorColumn = mapping.getColumnForRole(SchemaBase.Role.SUCCESSOR_INTERVAL_START);
    boolean successorOnMain =
        successorColumn.isPresent()
            && Objects.equals(successorColumn.get().first().getName(), mainTableName);

    // Step 1: capture the open row's surrogate PK (and reject early if no open row matches).
    StringBuilder findPk =
        new StringBuilder("SELECT ")
            .append(pkColumnName)
            .append(" FROM ")
            .append(mainTableName)
            .append(" WHERE ")
            .append(idColumnName)
            .append(" = ")
            .append(sqlString(featureId))
            .append(" AND ")
            .append(endColumnName)
            .append(" IS NULL");
    if (expectedStart.isPresent()) {
      findPk
          .append(" AND ")
          .append(startColumnName)
          .append(" = ")
          .append(sqlString(expectedStart.get().toString()));
    }
    findPk.append(" LIMIT 1;");
    List<String> oldPkRows;
    try {
      oldPkRows = sqlSession.runReturning(findPk.toString());
    } catch (RuntimeException e) {
      return builder.error(e).build();
    }
    if (oldPkRows.isEmpty()) {
      // No open version (concurrent retirement / unknown id) — or expectedStart mismatch (412).
      // Caller distinguishes by the presence of expectedStart.
      return builder.build();
    }
    String oldPkLit = sqlLiteralForPk(oldPkRows.get(0));

    // Step 1b: capture the open row's start, when needed for the predecessor denorm column.
    Optional<String> oldStart = Optional.empty();
    if (predecessorOnMain) {
      try {
        List<String> startRows =
            sqlSession.runReturning(
                "SELECT "
                    + startColumnName
                    + " FROM "
                    + mainTableName
                    + " WHERE "
                    + pkColumnName
                    + " = "
                    + oldPkLit
                    + ";");
        if (!startRows.isEmpty()) {
          oldStart = Optional.of(startRows.get(0));
        }
      } catch (RuntimeException e) {
        return builder.error(e).build();
      }
    }

    // Resolve which property updates land on the main table — those become inline overrides on the
    // clone INSERT, so the patches don't need a second UPDATE round-trip.
    Map<String, String> mainColumnPatches = new java.util.LinkedHashMap<>();
    for (FeatureTransactions.PropertyUpdate u : updates) {
      String joined = String.join(".", u.getPath());
      Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> resolved =
          mapping.getColumnForValue(joined, MappingRule.Scope.W);
      if (resolved.isPresent() && Objects.equals(resolved.get().first().getName(), mainTableName)) {
        SqlQueryColumn col = resolved.get().second();
        mainColumnPatches.put(col.getName(), encodeLiteral(col, u.getValue(), crs));
      }
    }

    // Step 2: clone the main row with literal overrides for role-bearing columns and inline
    // main-column patches; carry-forward everything else.
    List<String> insertCols = new ArrayList<>();
    List<String> selectExprs = new ArrayList<>();
    for (SqlQueryColumn col : mainTable.getColumns()) {
      String name = col.getName();
      if (Objects.equals(name, pkColumnName)) {
        continue; // auto-PK — let the DB generate the new value
      }
      insertCols.add(name);
      Optional<SchemaBase.Role> role = col.getRole();
      if (role.isPresent()) {
        if (role.get() == SchemaBase.Role.PRIMARY_INTERVAL_START) {
          selectExprs.add(tsLiteral);
          continue;
        }
        if (role.get() == SchemaBase.Role.PRIMARY_INTERVAL_END) {
          selectExprs.add("NULL");
          continue;
        }
        if (role.get() == SchemaBase.Role.PREDECESSOR_INTERVAL_START) {
          selectExprs.add(oldStart.map(SqlMutationSession::sqlString).orElse("NULL"));
          continue;
        }
        if (role.get() == SchemaBase.Role.SUCCESSOR_INTERVAL_START) {
          // The new row is open — no successor yet.
          selectExprs.add("NULL");
          continue;
        }
      }
      String patchLit = mainColumnPatches.get(name);
      selectExprs.add(patchLit != null ? patchLit : "m." + name);
    }
    String cloneMainSql =
        "INSERT INTO "
            + mainTableName
            + " ("
            + String.join(", ", insertCols)
            + ") SELECT "
            + String.join(", ", selectExprs)
            + " FROM "
            + mainTableName
            + " m WHERE m."
            + pkColumnName
            + " = "
            + oldPkLit
            + " RETURNING "
            + pkColumnName
            + ";";
    List<String> newPkRows;
    try {
      newPkRows = sqlSession.runReturning(cloneMainSql);
    } catch (RuntimeException e) {
      return builder.error(e).build();
    }
    if (newPkRows.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Clone-and-patch of feature id '"
                      + featureId
                      + "' in collection '"
                      + featureType
                      + "' did not return a new row PK; clone INSERT must have inserted 0 rows."))
          .build();
    }
    String newPkLit = sqlLiteralForPk(newPkRows.get(0));

    // Step 3: clone every writable junction table's rows for OLD_PK, redirecting the FK to NEW_PK.
    // Deduplicate junctions across writable-table entries (multiple property paths can map to one
    // junction).
    java.util.Set<String> clonedJunctions = new java.util.LinkedHashSet<>();
    for (SqlQuerySchema junction : mapping.getWritableTables().values()) {
      if (junction == null || Objects.equals(junction.getName(), mainTableName)) {
        continue;
      }
      if (!clonedJunctions.add(junction.getName())) {
        continue;
      }
      try {
        cloneJunctionRows(junction, oldPkLit, newPkLit);
      } catch (RuntimeException e) {
        return builder.error(e).build();
      }
    }

    // Step 4: retire OLD. Same guard as retireFeature — no-backdating + must be the open row.
    StringBuilder retireSet = new StringBuilder(endColumnName).append(" = ").append(tsLiteral);
    if (successorOnMain) {
      retireSet
          .append(", ")
          .append(successorColumn.get().second().getName())
          .append(" = ")
          .append(tsLiteral);
    }
    String retireSql =
        "UPDATE "
            + mainTableName
            + " SET "
            + retireSet
            + " WHERE "
            + pkColumnName
            + " = "
            + oldPkLit
            + " AND "
            + endColumnName
            + " IS NULL AND "
            + startColumnName
            + " < "
            + tsLiteral
            + " RETURNING "
            + pkColumnName
            + ";";
    List<String> retiredRows;
    try {
      retiredRows = sqlSession.runReturning(retireSql);
    } catch (RuntimeException e) {
      return builder.error(e).build();
    }
    if (retiredRows.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Clone-and-patch of feature id '"
                      + featureId
                      + "' in collection '"
                      + featureType
                      + "': failed to retire the previous open version (concurrent modification or"
                      + " no-backdating violation)."))
          .build();
    }

    // Step 5: patches on the new (now only) open row. Main-column patches already applied inline
    // during the clone (Step 2); only junction-backed patches need post-clone DELETE+INSERT. The
    // junction patch path uses `idCol = ? AND endCol IS NULL` to find the parent PK — that resolves
    // to NEW_PK now that OLD has been retired.
    List<FeatureTransactions.PropertyUpdate> junctionUpdates = new ArrayList<>();
    for (FeatureTransactions.PropertyUpdate u : updates) {
      String joined = String.join(".", u.getPath());
      Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> resolved =
          mapping.getColumnForValue(joined, MappingRule.Scope.W);
      boolean onMain =
          resolved.isPresent() && Objects.equals(resolved.get().first().getName(), mainTableName);
      if (!onMain) {
        junctionUpdates.add(u);
      }
    }
    if (!junctionUpdates.isEmpty()) {
      FeatureTransactions.MutationResult patchResult =
          patchInternal(
              featureType,
              featureId,
              junctionUpdates,
              crs,
              " AND " + endColumnName + " IS NULL",
              "open version of feature");
      if (patchResult.getError().isPresent()) {
        return builder.error(patchResult.getError().get()).build();
      }
    }

    builder.addIds(featureId);
    return builder.build();
  }

  // Clone every row of `junction` whose FK = oldPkLit into a new row whose FK = newPkLit. Carries
  // every other column forward. The junction's own auto-PK column (sortKey) is omitted so the DB
  // generates fresh values per cloned row.
  private void cloneJunctionRows(SqlQuerySchema junction, String oldPkLit, String newPkLit) {
    if (junction.getRelations().isEmpty()) {
      return;
    }
    SqlQueryJoin join = junction.getRelations().get(0);
    String fkColumn = join.getTargetField();
    String junctionPk = junction.getSortKey();
    List<String> insertCols = new ArrayList<>();
    List<String> selectExprs = new ArrayList<>();
    boolean fkSeen = false;
    for (SqlQueryColumn col : junction.getColumns()) {
      String name = col.getName();
      if (Objects.equals(name, junctionPk)) {
        continue; // auto-PK on the junction itself
      }
      insertCols.add(name);
      if (Objects.equals(name, fkColumn)) {
        selectExprs.add(newPkLit);
        fkSeen = true;
      } else {
        selectExprs.add(name);
      }
    }
    if (!fkSeen) {
      // The FK column may live outside getColumns() when the schema mapping doesn't expose it as a
      // data column (it's only the relation key). Add it explicitly so the cloned row is reachable.
      insertCols.add(fkColumn);
      selectExprs.add(newPkLit);
    }
    if (insertCols.isEmpty()) {
      return;
    }
    String sql =
        "INSERT INTO "
            + junction.getName()
            + " ("
            + String.join(", ", insertCols)
            + ") SELECT "
            + String.join(", ", selectExprs)
            + " FROM "
            + junction.getName()
            + " WHERE "
            + fkColumn
            + " = "
            + oldPkLit
            + ";";
    sqlSession.runReturning(sql);
  }

  // Format a captured PK value (returned by SqlSession.runReturning as a String) for inlining as
  // a SQL literal. Surrogate PKs are typically auto-increment integers; fall back to quoting for
  // anything that isn't a plain integer.
  private static String sqlLiteralForPk(String raw) {
    if (raw == null) {
      return "NULL";
    }
    try {
      Long.parseLong(raw);
      return raw;
    } catch (NumberFormatException ignored) {
      return sqlString(raw);
    }
  }

  private FeatureTransactions.MutationResult patchInternal(
      String featureType,
      String featureId,
      List<FeatureTransactions.PropertyUpdate> updates,
      EpsgCrs crs,
      String extraWherePredicate,
      String missingTargetLabel) {
    SqlQueryMapping mapping = requireMapping(featureType);
    ImmutableMutationResult.Builder builder =
        ImmutableMutationResult.builder()
            .type(FeatureTransactions.MutationResult.Type.UPDATE)
            .hasFeatures(false);
    if (updates.isEmpty()) {
      return builder.build();
    }

    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>> idColumn =
        mapping.getColumnForId();
    if (idColumn.isEmpty()) {
      return builder
          .error(
              new IllegalStateException(
                  "Feature type '" + featureType + "' has no id column; cannot patch in place."))
          .build();
    }
    SqlQuerySchema mainTable = mapping.getMainTable();
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
        Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
            resolved = mapping.getColumnForValue(joined, MappingRule.Scope.W);
        if (resolved.isPresent()) {
          SqlQuerySchema table = resolved.get().first();
          SqlQueryColumn column = resolved.get().second();
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
        Optional<SqlQuerySchema> objectTable = mapping.getTableForObject(joined);
        FeatureSchema objectSchema = resolveSchemaByPath(mapping.getMainSchema(), update.getPath());
        if (objectTable.isPresent()
            && objectSchema != null
            && objectSchema.getType() == SchemaBase.Type.OBJECT_ARRAY
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
                + extraWherePredicate
                + " RETURNING "
                + idColumnName
                + ";";
        List<String> returned = sqlSession.runReturning(sql);
        if (returned.isEmpty()) {
          return builder
              .error(
                  new IllegalArgumentException(
                      "No "
                          + missingTargetLabel
                          + " with id '"
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
        runJunctionPatch(patch, mainTableName, idColumnName, idLiteral, extraWherePredicate, crs);
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
                    + extraWherePredicate
                    + ";");
        if (exists.isEmpty()) {
          return builder
              .error(
                  new IllegalArgumentException(
                      "No "
                          + missingTargetLabel
                          + " with id '"
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
      String extraWherePredicate,
      EpsgCrs crs) {
    // In SqlQueryJoin (read from the child/junction's perspective): `sourceField` is on the
    // PARENT (its primary/sort key) and `targetField` is on the CHILD (the FK back to parent).
    // See SqlInsertGenerator2 line ~132 (`parent.sourceField` used as the parent sort key) and
    // line ~133 (`targetField` added to the child's column list).
    SqlQueryJoin join = patch.junction.getRelations().get(0);
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
            + extraWherePredicate
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
              + qualifyAliasPredicate(extraWherePredicate, "m")
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
              + qualifyAliasPredicate(extraWherePredicate, "m")
              + ";";
      sqlSession.runReturning(insertSql);
    }
  }

  // The junction subqueries alias the main table as `m`, so the extra predicate's bare column
  // references must be qualified with that alias. Single-pass replace works because the predicate
  // is generated by us (`" AND <colName> IS NULL"`); not robust against arbitrary user input.
  private static String qualifyAliasPredicate(String extraPredicate, String alias) {
    if (extraPredicate.isEmpty()) return extraPredicate;
    // Strip leading " AND " and prefix every word-start with the alias.
    int and = extraPredicate.indexOf("AND ");
    if (and < 0) return extraPredicate;
    String rest = extraPredicate.substring(and + 4);
    return " AND " + alias + "." + rest;
  }

  // Patch state for a junction-backed property. Two modes are encoded in the same record so the
  // executor's per-path map can hold both kinds:
  //   VALUE_ARRAY: `valueColumn` is the single value column; `objectChildColumns == null`.
  //   OBJECT_ARRAY: `valueColumn == null`; `objectChildColumns` maps child schema-ids to their
  //                 columns on the junction (in declaration order so SQL output is deterministic).
  private static final class JunctionPatch {
    final SqlQuerySchema junction;
    final SqlQueryColumn valueColumn;
    final java.util.LinkedHashMap<String, SqlQueryColumn> objectChildColumns;
    final String objectPath;
    final List<com.fasterxml.jackson.databind.JsonNode> values = new ArrayList<>();

    private JunctionPatch(
        SqlQuerySchema junction,
        SqlQueryColumn valueColumn,
        java.util.LinkedHashMap<String, SqlQueryColumn> objectChildColumns,
        String objectPath) {
      this.junction = junction;
      this.valueColumn = valueColumn;
      this.objectChildColumns = objectChildColumns;
      this.objectPath = objectPath;
    }

    static JunctionPatch valueArray(SqlQuerySchema junction, SqlQueryColumn valueColumn) {
      return new JunctionPatch(junction, valueColumn, null, null);
    }

    static JunctionPatch objectArray(
        SqlQuerySchema junction, FeatureSchema objectSchema, SqlQueryMapping mapping, String path) {
      java.util.LinkedHashMap<String, SqlQueryColumn> cols = new java.util.LinkedHashMap<>();
      for (FeatureSchema child : objectSchema.getProperties()) {
        if (child.getType() == SchemaBase.Type.OBJECT
            || child.getType() == SchemaBase.Type.OBJECT_ARRAY) {
          // Skip nested objects — only flat scalar children are supported in this phase. The
          // caller will see a NULL for those keys, or an error if the user sets them.
          continue;
        }
        String childPath = path + "." + child.getName();
        mapping
            .getColumnForValue(childPath, MappingRule.Scope.W)
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
      SqlQueryColumn column,
      Optional<com.fasterxml.jackson.databind.JsonNode> valueOpt,
      EpsgCrs crs) {
    if (valueOpt.isEmpty() || valueOpt.get().isNull()) {
      return "NULL";
    }
    com.fasterxml.jackson.databind.JsonNode value = valueOpt.get();
    if (column.hasOperation(SqlQueryColumn.Operation.WKT)
        || column.hasOperation(SqlQueryColumn.Operation.WKB)) {
      return encodeGeometryLiteral(column, value, crs);
    }
    SchemaBase.Type type = column.getType();
    if (type == SchemaBase.Type.STRING
        || type == SchemaBase.Type.DATETIME
        || type == SchemaBase.Type.DATE) {
      return sqlString(value.asText());
    }
    if (type == SchemaBase.Type.BOOLEAN) {
      return value.asBoolean() ? "TRUE" : "FALSE";
    }
    if (type == SchemaBase.Type.INTEGER || type == SchemaBase.Type.FLOAT) {
      return value.asText();
    }
    // Fallback — treat as string. Avoids generating invalid SQL on niche column types we haven't
    // wired up explicitly yet (FEATURE_REF, etc.).
    return sqlString(value.asText());
  }

  private String encodeGeometryLiteral(
      SqlQueryColumn column, com.fasterxml.jackson.databind.JsonNode value, EpsgCrs crs) {
    if (!value.isObject()) {
      throw new IllegalArgumentException(
          "Geometry property '"
              + column.getName()
              + "' requires a GeoJSON geometry object as the value, got: "
              + value.getNodeType());
    }
    Geometry<?> geometry;
    try {
      geometry =
          new GeometryDecoderJson(true).decode(value, Optional.ofNullable(crs), Optional.empty());
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException(
          "Could not parse GeoJSON geometry for property '"
              + column.getName()
              + "': "
              + e.getMessage(),
          e);
    }
    if (crs != null && !Objects.equals(crs, nativeCrs)) {
      Optional<CrsTransformer> transformer = crsTransformerFactory.getTransformer(crs, nativeCrs);
      if (transformer.isPresent()) {
        geometry =
            geometry.accept(
                new CoordinatesTransformer(
                    ImmutableCrsTransform.of(Optional.empty(), transformer.get())));
      }
    }
    String wkt;
    try {
      wkt = new GeometryEncoderWkt().encode(geometry);
    } catch (java.io.IOException e) {
      throw new IllegalStateException(
          "Could not encode geometry as WKT for property '"
              + column.getName()
              + "': "
              + e.getMessage(),
          e);
    }
    String result = String.format("ST_GeomFromText('%s',%s)", wkt, nativeCrs.getCode());
    if (geometry.getType() == GeometryType.POLYGON
        || geometry.getType() == GeometryType.MULTI_POLYGON) {
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
    Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
        roleIdColumn = mapping.getColumnForId();
    String roleIdColumnName = roleIdColumn.map(t -> t.second().getName()).orElse(null);
    SqlQuerySchema roleIdTable =
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
      SqlQuerySchema roleIdTable,
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
      SqlQuerySchema roleIdTable,
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
      FeatureDataSql feature, SqlQuerySchema roleIdTable, String roleIdColumnName) {
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
