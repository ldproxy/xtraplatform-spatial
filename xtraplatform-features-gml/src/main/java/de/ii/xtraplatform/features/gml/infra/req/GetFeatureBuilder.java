/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.gml.infra.req.GetFeature.ResultType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFeatureBuilder {
  private final List<WfsQuery> queries;
  private Integer count;
  private Integer startIndex;
  private boolean hitsOnly;
  private final Map<String, String> additionalOperationParameters;

  public GetFeatureBuilder() {
    this.queries = new ArrayList<>();
    this.additionalOperationParameters = new HashMap<>();
  }

  public GetFeatureBuilder query(WfsQuery query) {
    this.queries.add(query);
    return this;
  }

  public GetFeatureBuilder count(Integer count) {
    this.count = count;
    return this;
  }

  public GetFeatureBuilder startIndex(Integer startIndex) {
    this.startIndex = startIndex;
    return this;
  }

  public GetFeatureBuilder hitsOnly() {
    this.hitsOnly = true;
    return this;
  }

  public GetFeatureBuilder additionalOperationParameters(
      Map<String, String> additionalOperationParameters) {
    this.additionalOperationParameters.putAll(additionalOperationParameters);
    return this;
  }

  public GetFeature build() {
    return new GetFeature(
        ImmutableList.copyOf(queries),
        count,
        startIndex,
        hitsOnly ? ResultType.HITS : ResultType.RESULT,
        ImmutableMap.copyOf(additionalOperationParameters));
  }
}
