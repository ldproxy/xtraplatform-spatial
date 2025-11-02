/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.ImmutableResult.Builder;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class FeatureTokenTransformerHooks extends FeatureTokenTransformer {

  private final CompletableFuture<CollectionMetadata> onCollectionMetadata;
  private final Consumer<Boolean> hasFeaturesSetter;
  private boolean done;

  public FeatureTokenTransformerHooks(
      Builder resultBuilder, CompletableFuture<CollectionMetadata> onCollectionMetadata) {
    this.onCollectionMetadata = onCollectionMetadata;
    this.hasFeaturesSetter = resultBuilder::hasFeatures;
    this.done = false;
  }

  public <X> FeatureTokenTransformerHooks(
      ImmutableResultReduced.Builder<X> resultBuilder,
      CompletableFuture<CollectionMetadata> onCollectionMetadata) {
    this.onCollectionMetadata = onCollectionMetadata;
    this.hasFeaturesSetter = resultBuilder::hasFeatures;
    this.done = false;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    onCollectionMetadata.complete(context.metadata());

    super.onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!done) {
      hasFeaturesSetter.accept(true);
      this.done = true;
    }

    super.onFeatureStart(context);
  }
}
