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
