/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.InResultSet;
import de.ii.xtraplatform.cql.domain.InResultSetByKey;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Or;
import java.util.List;
import java.util.Map;

/**
 * Three-valued evaluation of a filter against the result sets materialized so far, used by {@link
 * ResultSetMaterializer} to skip producers whose result is known to be empty.
 */
final class ResultSetTruth {

  private enum Truth {
    TRUE,
    FALSE,
    UNKNOWN
  }

  private ResultSetTruth() {}

  /**
   * True if the filter can only ever be false given the result sets materialized so far, i.e. it
   * references a set that materialized to no members in a boolean position that forces the whole
   * filter false. Conservative: any leaf whose value is not pinned by an empty set is treated as
   * indeterminate, so a producer is skipped only when boolean algebra guarantees an empty result.
   */
  static boolean isProvablyEmpty(
      Cql2Expression filter,
      Map<String, List<Object>> materialized,
      Map<String, List<List<Object>>> materializedKeys) {
    return truth(filter, materialized, materializedKeys) == Truth.FALSE;
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private static Truth truth(
      CqlNode node,
      Map<String, List<Object>> materialized,
      Map<String, List<List<Object>>> materializedKeys) {
    if (node instanceof InResultSetByKey) {
      List<List<Object>> keys = materializedKeys.get(((InResultSetByKey) node).getSetName());
      // as below: a materialized-empty set makes the semi-join always false
      return keys != null && keys.isEmpty() ? Truth.FALSE : Truth.UNKNOWN;
    }
    if (node instanceof InResultSet) {
      List<Object> values = materialized.get(((InResultSet) node).getSetName());
      // a materialized-empty set makes the IN / A_OVERLAPS predicate always false; a non-empty or
      // not-yet-materialized (oversized) set is indeterminate at the query level
      return values != null && values.isEmpty() ? Truth.FALSE : Truth.UNKNOWN;
    }
    if (node instanceof And) {
      Truth result = Truth.TRUE;
      for (Cql2Expression child : ((And) node).getArgs()) {
        Truth childTruth = truth(child, materialized, materializedKeys);
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
        Truth childTruth = truth(child, materialized, materializedKeys);
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
      Truth child = truth(((Not) node).getArgs().get(0), materialized, materializedKeys);
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
}
