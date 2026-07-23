/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface MultiFeatureQuery extends Query {

  @Value.Immutable
  interface SubQuery extends TypeQuery {
    String getCollectionId();
  }

  List<SubQuery> getQueries();

  /**
   * If enabled, a feature that is selected by more than one query is only included in the response
   * once.
   */
  @Value.Default
  default boolean getDeduplicate() {
    return false;
  }

  /**
   * If enabled (the default), the query is executed with paging support: {@code numberReturned} and
   * {@code numberMatched} are computed and {@code limit}/{@code offset} select a page of the result
   * set. If disabled, the query is executed single-shot: all matching features are returned in one
   * pass, without meta queries and without {@code numberReturned}/{@code numberMatched}.
   */
  @Value.Default
  default boolean getSupportPaging() {
    return true;
  }

  /**
   * In single-shot mode (paging disabled), an optional maximum number of features read per
   * sub-query. {@code 0} means no limit.
   */
  @Value.Default
  default int getMaxFeaturesPerSubQuery() {
    return 0;
  }
}
