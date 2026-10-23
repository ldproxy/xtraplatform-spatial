/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

public class FeatureTokenTransformerProgress extends FeatureTokenTransformer {

  private final JobHook jobHook;
  private boolean countFeatures;

  public FeatureTokenTransformerProgress(JobHook jobHook) {
    this.jobHook = jobHook;
    this.countFeatures = false;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    // features are only counted when the total is known (paged queries with a meta phase);
    // unpaged queries report sub-query progress in the connector instead
    if (context.metadata().getNumberReturned().isPresent()) {
      long numberReturned = context.metadata().getNumberReturned().getAsLong();
      jobHook.init((int) Math.min(Integer.MAX_VALUE, numberReturned));
      this.countFeatures = true;
    }

    super.onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    jobHook.checkpoint();

    if (countFeatures) {
      jobHook.update(1);
    }

    super.onFeatureStart(context);
  }
}
