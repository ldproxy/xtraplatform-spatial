/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.base.domain.ETag;
import de.ii.xtraplatform.base.domain.ETag.Type;
import de.ii.xtraplatform.features.domain.ImmutableResult.Builder;
import java.util.Objects;
import java.util.function.Consumer;
import jakarta.ws.rs.core.EntityTag;

public class FeatureTokenTransformerWeakETag extends FeatureTokenTransformer {

  private final Consumer<EntityTag> builder;
  private final ETag.Incremental eTag;

  public FeatureTokenTransformerWeakETag(Builder resultBuilder) {
    this.builder = resultBuilder::eTag;
    this.eTag = ETag.incremental();
  }

  public <X> FeatureTokenTransformerWeakETag(ImmutableResultReduced.Builder<X> resultBuilder) {
    this.builder = resultBuilder::eTag;
    this.eTag = ETag.incremental();
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    builder.accept(eTag.build(Type.WEAK));

    super.onEnd(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (Objects.nonNull(context.value())) {
      eTag.put(context.value());
    }

    super.onValue(context);
  }
}
