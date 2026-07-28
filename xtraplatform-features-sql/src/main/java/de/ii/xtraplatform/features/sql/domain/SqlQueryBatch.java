/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface SqlQueryBatch {

  long getLimit();

  long getOffset();

  long getChunkSize();

  @Value.Default
  default boolean isSingleFeature() {
    return false;
  }

  /**
   * Single-shot (unpaged) mode: every matching row is read in one pass per (sub-query, table)
   * without a meta query, without chunking, and without a key-range window. {@code limit} does not
   * page and {@code numberReturned}/{@code numberMatched} are not computed.
   */
  @Value.Default
  default boolean isUnpaged() {
    return false;
  }

  /**
   * Whether {@code numberMatched} is reported. When enabled for a multi-query, the count is
   * computed for every sub-query (not only those contributing to the current page) so that the
   * reported {@code numberMatched} is the invariant total across all sub-queries; the value queries
   * are still executed only for the sub-queries needed for the page.
   */
  @Value.Default
  default boolean isComputeNumberMatched() {
    return false;
  }

  List<SqlQuerySet> getQuerySets();
}
