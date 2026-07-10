/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.BinaryScalarOperation;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorCopy;
import de.ii.xtraplatform.cql.domain.ImmutableInResultSet;
import de.ii.xtraplatform.cql.domain.InResultSet;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Or;
import de.ii.xtraplatform.features.domain.ImmutableMultiFeatureQuery;
import de.ii.xtraplatform.features.domain.ImmutableSubQuery;
import de.ii.xtraplatform.features.domain.MultiFeatureQuery;
import de.ii.xtraplatform.features.domain.MultiFeatureQuery.SubQuery;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materializes the result sets of a multi-query once per response. Each result set's producer is
 * run a single time (its dependencies already materialized as literal id lists), and the collected
 * values are attached to the {@link InResultSet} nodes of the consuming filters so that they are
 * encoded as a literal {@code IN} list instead of a per-statement nested subquery. A result set
 * that exceeds the configured cap is materialized once into an indexed table that consumers join
 * (when the dialect supports it); otherwise it is left unmaterialized and falls back to the inline
 * (CTE) re-evaluation.
 */
public class ResultSetMaterializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetMaterializer.class);

  // prefix for the request-scoped tables that hold oversized result sets
  private static final String TABLE_PREFIX = "_rs_mat_";
  // monotonic counter making each materialize() call's table names unique within this JVM
  private static final AtomicLong SEQUENCE = new AtomicLong();

  private final Supplier<SqlClient> sqlClient;
  private final FilterEncoderSql filterEncoder;
  private final int maxSetSize;
  private final SqlDialect dialect;
  // random per-provider-instance token so concurrent instances on the same database cannot collide
  private final String instanceId;

  public ResultSetMaterializer(
      Supplier<SqlClient> sqlClient,
      FilterEncoderSql filterEncoder,
      int maxSetSize,
      SqlDialect dialect) {
    this.sqlClient = sqlClient;
    this.filterEncoder = filterEncoder;
    this.maxSetSize = maxSetSize;
    this.dialect = dialect;
    this.instanceId = Integer.toHexString(ThreadLocalRandom.current().nextInt());
  }

  /**
   * Returns a copy of the query with every materializable result set computed and inlined. If the
   * query uses no result sets, it is returned unchanged. Any tables created for oversized sets are
   * named uniquely per call; their names are not tracked and must be dropped by the caller via the
   * overload that collects them.
   */
  public MultiFeatureQuery materialize(MultiFeatureQuery query) {
    return materialize(query, new ArrayList<>());
  }

  /**
   * As {@link #materialize(MultiFeatureQuery)}, but records the names of any tables created for
   * oversized result sets into {@code createdTables}. The caller owns their lifecycle and must
   * {@link #dropTables(java.util.Collection) drop} them once the query's stream has completed.
   */
  public MultiFeatureQuery materialize(MultiFeatureQuery query, List<String> createdTables) {
    Map<String, InResultSet> sets = new LinkedHashMap<>();
    for (SubQuery subQuery : query.getQueries()) {
      for (Cql2Expression filter : subQuery.getFilters()) {
        collect(filter, sets);
      }
    }
    if (sets.isEmpty()) {
      return query;
    }

    // one sequence value per call gives every oversized set in this request a name that is unique
    // across concurrent requests (and, with the instance token, across provider instances)
    long sequence = SEQUENCE.incrementAndGet();
    Map<String, List<Object>> materialized = new HashMap<>();
    // oversized sets materialized into a request-scoped table: set name -> table name
    Map<String, String> materializedTables = new HashMap<>();
    int shortCircuited = 0;
    // materialize level by level: within a level the producers are independent and run concurrently
    // (bounded by the connection pool). SQL is built single-threaded between levels, so the filter
    // encoder is never invoked concurrently and the materialized map is only mutated on this
    // thread.
    for (List<String> level : topologicalLevels(sets)) {
      Map<String, CompletableFuture<Collection<SqlRow>>> running = new LinkedHashMap<>();
      Map<String, SchemaBase.Type> valueTypes = new HashMap<>();
      // keep each level's prepared nodes (dependencies already applied) for the join phase, where
      // an
      // oversized set is materialized into a table from the very same producer
      Map<String, InResultSet> prepared = new HashMap<>();
      for (String name : level) {
        InResultSet node = sets.get(name);
        // if a dependency has already materialized to no members in a position that forces this
        // producer's filter false, the producer can only be empty too — record the empty set and
        // skip its query. Consumers encode IN () for it either way, so the output is unchanged.
        if (node.getProducerFilter().isPresent()
            && isProvablyEmpty(node.getProducerFilter().get(), materialized)) {
          materialized.put(name, List.of());
          shortCircuited++;
          continue;
        }
        InResultSet preparedNode =
            node.getProducerFilter().isPresent()
                ? new ImmutableInResultSet.Builder()
                    .from(node)
                    .producerFilter(
                        applyMaterialized(
                            node.getProducerFilter().get(), materialized, materializedTables))
                    .build()
                : node;
        prepared.put(name, preparedNode);

        // bound the fetch to one past the cap so an oversized set is detected without loading it
        // all
        String producerQuery =
            filterEncoder.encodeResultSetProducer(preparedNode) + " LIMIT " + (maxSetSize + 1);
        valueTypes.put(name, filterEncoder.resultSetValueType(node));
        running.put(name, sqlClient.get().run(producerQuery, SqlQueryOptions.single()));
      }

      for (Map.Entry<String, CompletableFuture<Collection<SqlRow>>> entry : running.entrySet()) {
        String name = entry.getKey();
        Collection<SqlRow> rows = entry.getValue().join();
        if (rows.size() > maxSetSize) {
          if (dialect.supportsResultSetTables()) {
            // too large to inline as a literal list: materialize the producer once into an indexed
            // table; consumers reference it instead of re-deriving the producer each time
            materializeTable(name, prepared.get(name), sequence, materializedTables, createdTables);
          } else if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(
                "Result set '{}' has more than the materialization cap of {} members; falling back"
                    + " to inline evaluation for this set.",
                name,
                maxSetSize);
          }
          continue;
        }
        List<Object> values =
            rows.stream()
                .map(row -> coerce(row.getValues().get(0), valueTypes.get(name)))
                .distinct()
                .collect(Collectors.toList());
        materialized.put(name, values);
      }
    }

    if (shortCircuited > 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Short-circuited {} of {} result-set producer(s) with an empty dependency.",
          shortCircuited,
          sets.size());
    }

    List<SubQuery> rewritten =
        query.getQueries().stream()
            .map(
                subQuery ->
                    (SubQuery)
                        ImmutableSubQuery.builder()
                            .from(subQuery)
                            .filters(
                                subQuery.getFilters().stream()
                                    .map(
                                        filter ->
                                            applyMaterialized(
                                                filter, materialized, materializedTables))
                                    .collect(Collectors.toList()))
                            .build())
            .collect(Collectors.toList());

    return ImmutableMultiFeatureQuery.builder().from(query).queries(rewritten).build();
  }

  /** True if the query contains at least one result-set reference. */
  public static boolean hasResultSets(MultiFeatureQuery query) {
    Map<String, InResultSet> sets = new LinkedHashMap<>();
    for (SubQuery subQuery : query.getQueries()) {
      for (Cql2Expression filter : subQuery.getFilters()) {
        collect(filter, sets);
        if (!sets.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  private static void collect(Cql2Expression expression, Map<String, InResultSet> sets) {
    Collector collector = new Collector();
    expression.accept(collector);
    for (InResultSet node : collector.found) {
      if (!sets.containsKey(node.getSetName())) {
        sets.put(node.getSetName(), node);
        node.getProducerFilter().ifPresent(filter -> collect(filter, sets));
      }
    }
  }

  /**
   * Groups the result sets into dependency levels: level 0 has no dependencies, level n depends
   * only on sets in earlier levels. Sets within a level are independent and may be materialized
   * concurrently.
   */
  private List<List<String>> topologicalLevels(Map<String, InResultSet> sets) {
    Map<String, Set<String>> deps = new HashMap<>();
    for (String name : sets.keySet()) {
      deps.put(name, dependenciesOf(sets.get(name), sets));
    }
    List<List<String>> levels = new ArrayList<>();
    Set<String> done = new HashSet<>();
    while (done.size() < sets.size()) {
      List<String> level =
          sets.keySet().stream()
              .filter(name -> !done.contains(name) && done.containsAll(deps.get(name)))
              .collect(Collectors.toList());
      if (level.isEmpty()) {
        // no progress (should not happen for forward-only references) — emit the rest as one level
        level =
            sets.keySet().stream()
                .filter(name -> !done.contains(name))
                .collect(Collectors.toList());
      }
      levels.add(level);
      done.addAll(level);
    }
    return levels;
  }

  private static Set<String> dependenciesOf(InResultSet node, Map<String, InResultSet> sets) {
    if (node.getProducerFilter().isEmpty()) {
      return Set.of();
    }
    Collector collector = new Collector();
    node.getProducerFilter().get().accept(collector);
    Set<String> deps = new HashSet<>();
    for (InResultSet dependency : collector.found) {
      if (sets.containsKey(dependency.getSetName())) {
        deps.add(dependency.getSetName());
      }
    }
    return deps;
  }

  private static Cql2Expression applyMaterialized(
      Cql2Expression expression,
      Map<String, List<Object>> materialized,
      Map<String, String> materializedTables) {
    return (Cql2Expression)
        expression.accept(new ApplyMaterialized(materialized, materializedTables));
  }

  /**
   * Materializes an oversized producer into an indexed table that consumers reference instead of
   * re-deriving the producer. The table name is unique to this materialize call (instance token +
   * call sequence + set name), so concurrent requests never collide; the name is recorded in {@code
   * createdTables} so the caller can drop it once the query's stream has completed.
   */
  private void materializeTable(
      String name,
      InResultSet prepared,
      long sequence,
      Map<String, String> materializedTables,
      List<String> createdTables) {
    String suffix = name.replaceAll("[^A-Za-z0-9_]", "_").toLowerCase(Locale.ROOT);
    if (suffix.length() > 24) {
      suffix = suffix.substring(0, 24);
    }
    String table = TABLE_PREFIX + instanceId + "_" + sequence + "_" + suffix;
    SqlClient client = sqlClient.get();
    client
        .run(
            dialect.createResultSetTable(
                table, filterEncoder.encodeResultSetProducerAliased(prepared)),
            SqlQueryOptions.ddl())
        .join();
    client
        .run(
            dialect.createResultSetTableIndex(table, FilterEncoderSql.RESULT_SET_VALUE_COLUMN),
            SqlQueryOptions.ddl())
        .join();
    materializedTables.put(name, table);
    createdTables.add(table);
  }

  /** Drops the given result-set tables, best effort. Safe to call with an empty collection. */
  public void dropTables(java.util.Collection<String> tables) {
    for (String table : tables) {
      try {
        sqlClient.get().run(dialect.dropResultSetTable(table), SqlQueryOptions.ddl()).join();
      } catch (RuntimeException e) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Could not drop result-set table '{}': {}", table, e.getMessage());
        }
      }
    }
  }

  /**
   * True if the filter can only ever be false given the result sets materialized so far, i.e. it
   * references a set that materialized to no members in a boolean position that forces the whole
   * filter false. Conservative: any leaf whose value is not pinned by an empty set is treated as
   * indeterminate, so a producer is skipped only when boolean algebra guarantees an empty result.
   */
  private static boolean isProvablyEmpty(
      Cql2Expression filter, Map<String, List<Object>> materialized) {
    return truth(filter, materialized) == Truth.FALSE;
  }

  private enum Truth {
    TRUE,
    FALSE,
    UNKNOWN
  }

  private static Truth truth(CqlNode node, Map<String, List<Object>> materialized) {
    if (node instanceof InResultSet) {
      List<Object> values = materialized.get(((InResultSet) node).getSetName());
      // a materialized-empty set makes the IN / A_OVERLAPS predicate always false; a non-empty or
      // not-yet-materialized (oversized) set is indeterminate at the query level
      return values != null && values.isEmpty() ? Truth.FALSE : Truth.UNKNOWN;
    }
    if (node instanceof And) {
      Truth result = Truth.TRUE;
      for (Cql2Expression child : ((And) node).getArgs()) {
        Truth childTruth = truth(child, materialized);
        if (childTruth == Truth.FALSE) {
          return Truth.FALSE;
        }
        if (childTruth == Truth.UNKNOWN) {
          result = Truth.UNKNOWN;
        }
      }
      return result;
    }
    if (node instanceof Or) {
      Truth result = Truth.FALSE;
      for (Cql2Expression child : ((Or) node).getArgs()) {
        Truth childTruth = truth(child, materialized);
        if (childTruth == Truth.TRUE) {
          return Truth.TRUE;
        }
        if (childTruth == Truth.UNKNOWN) {
          result = Truth.UNKNOWN;
        }
      }
      return result;
    }
    if (node instanceof Not) {
      Truth child = truth(((Not) node).getArgs().get(0), materialized);
      if (child == Truth.TRUE) {
        return Truth.FALSE;
      }
      if (child == Truth.FALSE) {
        return Truth.TRUE;
      }
      return Truth.UNKNOWN;
    }
    return Truth.UNKNOWN;
  }

  /**
   * True if the filter can only ever be false given the result sets materialized so far, i.e. it
   * references a set that materialized to no members in a boolean position that forces the whole
   * filter false. Conservative: any leaf whose value is not pinned by an empty set is treated as
   * indeterminate, so a producer is skipped only when boolean algebra guarantees an empty result.
   */
  private static boolean isProvablyEmpty(
      Cql2Expression filter, Map<String, List<Object>> materialized) {
    return truth(filter, materialized) == Truth.FALSE;
  }

  private enum Truth {
    TRUE,
    FALSE,
    UNKNOWN
  }

  private static Truth truth(CqlNode node, Map<String, List<Object>> materialized) {
    if (node instanceof InResultSet) {
      List<Object> values = materialized.get(((InResultSet) node).getSetName());
      // a materialized-empty set makes the IN / A_OVERLAPS predicate always false; a non-empty or
      // not-yet-materialized (oversized) set is indeterminate at the query level
      return values != null && values.isEmpty() ? Truth.FALSE : Truth.UNKNOWN;
    }
    if (node instanceof And) {
      Truth result = Truth.TRUE;
      for (Cql2Expression child : ((And) node).getArgs()) {
        Truth childTruth = truth(child, materialized);
        if (childTruth == Truth.FALSE) {
          return Truth.FALSE;
        }
        if (childTruth == Truth.UNKNOWN) {
          result = Truth.UNKNOWN;
        }
      }
      return result;
    }
    if (node instanceof Or) {
      Truth result = Truth.FALSE;
      for (Cql2Expression child : ((Or) node).getArgs()) {
        Truth childTruth = truth(child, materialized);
        if (childTruth == Truth.TRUE) {
          return Truth.TRUE;
        }
        if (childTruth == Truth.UNKNOWN) {
          result = Truth.UNKNOWN;
        }
      }
      return result;
    }
    if (node instanceof Not) {
      Truth child = truth(((Not) node).getArgs().get(0), materialized);
      if (child == Truth.TRUE) {
        return Truth.FALSE;
      }
      if (child == Truth.FALSE) {
        return Truth.TRUE;
      }
      return Truth.UNKNOWN;
    }
    return Truth.UNKNOWN;
  }

  private static Object coerce(Object value, SchemaBase.Type type) {
    if (!(value instanceof String)) {
      return value;
    }
    String string = (String) value;
    try {
      switch (type) {
        case INTEGER:
          return Long.parseLong(string);
        case FLOAT:
          return Double.parseDouble(string);
        case BOOLEAN:
          return Boolean.parseBoolean(string);
        default:
          return string;
      }
    } catch (NumberFormatException e) {
      return string;
    }
  }

  /** Records the {@link InResultSet} nodes encountered while traversing a filter. */
  private static class Collector extends CqlVisitorCopy {
    private final List<InResultSet> found = new ArrayList<>();

    @Override
    public CqlNode visit(BinaryScalarOperation scalarOperation, List<CqlNode> children) {
      CqlNode copy = super.visit(scalarOperation, children);
      if (copy instanceof InResultSet) {
        found.add((InResultSet) copy);
      }
      return copy;
    }
  }

  /** Attaches materialized values (or a materialized table) to the {@link InResultSet} nodes. */
  private static class ApplyMaterialized extends CqlVisitorCopy {
    private final Map<String, List<Object>> materialized;
    private final Map<String, String> materializedTables;

    ApplyMaterialized(
        Map<String, List<Object>> materialized, Map<String, String> materializedTables) {
      this.materialized = materialized;
      this.materializedTables = materializedTables;
    }

    @Override
    public CqlNode visit(BinaryScalarOperation scalarOperation, List<CqlNode> children) {
      CqlNode copy = super.visit(scalarOperation, children);
      if (copy instanceof InResultSet) {
        InResultSet node = (InResultSet) copy;
        List<Object> values = materialized.get(node.getSetName());
        if (values != null) {
          return new ImmutableInResultSet.Builder().from(node).materializedValues(values).build();
        }
        String table = materializedTables.get(node.getSetName());
        if (table != null) {
          return new ImmutableInResultSet.Builder().from(node).materializedTable(table).build();
        }
      }
      return copy;
    }
  }
}
