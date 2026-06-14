/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.cql.domain.BinaryScalarOperation;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorCopy;
import de.ii.xtraplatform.cql.domain.ImmutableInResultSet;
import de.ii.xtraplatform.cql.domain.InResultSet;
import de.ii.xtraplatform.features.domain.ImmutableMultiFeatureQuery;
import de.ii.xtraplatform.features.domain.ImmutableSubQuery;
import de.ii.xtraplatform.features.domain.MultiFeatureQuery;
import de.ii.xtraplatform.features.domain.MultiFeatureQuery.SubQuery;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materializes the result sets of a multi-query once per response. Each result set's producer is
 * run a single time (its dependencies already materialized as literal id lists), and the collected
 * values are attached to the {@link InResultSet} nodes of the consuming filters so that they are
 * encoded as a literal {@code IN} list instead of a per-statement nested subquery. A result set
 * that exceeds the configured cap is left unmaterialized and falls back to the inline (CTE)
 * encoding.
 */
public class ResultSetMaterializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetMaterializer.class);

  private final Supplier<SqlClient> sqlClient;
  private final FilterEncoderSql filterEncoder;
  private final int maxSetSize;

  public ResultSetMaterializer(
      Supplier<SqlClient> sqlClient, FilterEncoderSql filterEncoder, int maxSetSize) {
    this.sqlClient = sqlClient;
    this.filterEncoder = filterEncoder;
    this.maxSetSize = maxSetSize;
  }

  /**
   * Returns a copy of the query with every materializable result set computed and inlined. If the
   * query uses no result sets, it is returned unchanged.
   */
  public MultiFeatureQuery materialize(MultiFeatureQuery query) {
    Map<String, InResultSet> sets = new LinkedHashMap<>();
    for (SubQuery subQuery : query.getQueries()) {
      for (Cql2Expression filter : subQuery.getFilters()) {
        collect(filter, sets);
      }
    }
    if (sets.isEmpty()) {
      return query;
    }

    Map<String, List<Object>> materialized = new HashMap<>();
    // materialize level by level: within a level the producers are independent and run concurrently
    // (bounded by the connection pool). SQL is built single-threaded between levels, so the filter
    // encoder is never invoked concurrently and the materialized map is only mutated on this
    // thread.
    for (List<String> level : topologicalLevels(sets)) {
      Map<String, CompletableFuture<Collection<SqlRow>>> running = new LinkedHashMap<>();
      Map<String, SchemaBase.Type> valueTypes = new HashMap<>();
      for (String name : level) {
        InResultSet node = sets.get(name);
        InResultSet prepared =
            node.getProducerFilter().isPresent()
                ? new ImmutableInResultSet.Builder()
                    .from(node)
                    .producerFilter(applyMaterialized(node.getProducerFilter().get(), materialized))
                    .build()
                : node;

        // bound the fetch to one past the cap so an oversized set is detected without loading it
        // all
        String producerQuery =
            filterEncoder.encodeResultSetProducer(prepared) + " LIMIT " + (maxSetSize + 1);
        valueTypes.put(name, filterEncoder.resultSetValueType(node));
        running.put(name, sqlClient.get().run(producerQuery, SqlQueryOptions.single()));
      }

      for (Map.Entry<String, CompletableFuture<Collection<SqlRow>>> entry : running.entrySet()) {
        String name = entry.getKey();
        Collection<SqlRow> rows = entry.getValue().join();
        if (rows.size() > maxSetSize) {
          if (LOGGER.isWarnEnabled()) {
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

    List<SubQuery> rewritten =
        query.getQueries().stream()
            .map(
                subQuery ->
                    (SubQuery)
                        ImmutableSubQuery.builder()
                            .from(subQuery)
                            .filters(
                                subQuery.getFilters().stream()
                                    .map(filter -> applyMaterialized(filter, materialized))
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
      Cql2Expression expression, Map<String, List<Object>> materialized) {
    return (Cql2Expression) expression.accept(new ApplyMaterialized(materialized));
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

  /** Attaches materialized values to the {@link InResultSet} nodes that have them. */
  private static class ApplyMaterialized extends CqlVisitorCopy {
    private final Map<String, List<Object>> materialized;

    ApplyMaterialized(Map<String, List<Object>> materialized) {
      this.materialized = materialized;
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
      }
      return copy;
    }
  }
}
