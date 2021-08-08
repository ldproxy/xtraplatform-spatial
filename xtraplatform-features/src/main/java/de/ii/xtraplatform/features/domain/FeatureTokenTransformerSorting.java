/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: main table columns might have to wait for joined queries (likewise for deeper levels)
// joined queries might not have any rows
// if a row for a joined query with a greater order arrives, lesser joined queries can be assumed to be empty
//TODO: return nulls from FeatureDecoderSql
public class FeatureTokenTransformerSorting extends FeatureTokenTransformer {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(FeatureTokenTransformerSorting.class);

  private Map<List<String>, Integer> pathOrder;
  private int lastOrder;
  private List<Integer> lastIndexes;

  public FeatureTokenTransformerSorting() {
  }

  @Override
  public void onStart(ModifiableContext context) {
    this.pathOrder = new LinkedHashMap<>();
    int counter = 0;
    for (List<String> path : context.mapping().getTargetSchemasByPath().keySet()) {
      pathOrder.put(path, counter);
      LOGGER.warn("{}: {}", counter, path);
      counter++;
    }

    super.onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext context) {
    this.lastIndexes = ImmutableList.of();
    this.lastOrder = 0;

    super.onFeatureStart(context);
  }

  @Override
  public void onObjectStart(ModifiableContext context) {
    //LOGGER.warn("{} - {} - O", pathOrder.get(context.path()), context.path());

    if (pathOrder.containsKey(context.path())) {
      this.lastOrder = pathOrder.get(context.path());
    }

    this.lastIndexes = context.indexes();

    super.onObjectStart(context);
  }

  @Override
  public void onObjectEnd(ModifiableContext context) {
    super.onObjectEnd(context);
  }

  @Override
  public void onArrayStart(ModifiableContext context) {
    //LOGGER.warn("{} - {} - A", pathOrder.get(context.path()), context.path());

    super.onArrayStart(context);
  }

  @Override
  public void onArrayEnd(ModifiableContext context) {
    super.onArrayEnd(context);
  }

  @Override
  public void onValue(ModifiableContext context) {
    int order = pathOrder.get(context.path());

    if (order > lastOrder + 1 && Objects.equals(lastIndexes, context.indexes())) {
      LOGGER.warn("{} - {} - waiting for {}", order, context.path(), lastOrder + 1);
    }

    this.lastOrder = order;

    super.onValue(context);
  }
}
