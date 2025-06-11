/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.features.domain.FeatureTokenContext;
import de.ii.xtraplatform.features.domain.FeatureTokenSource;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaMappingBase;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.GenericContextSimple;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.streams.domain.Reactive.Source;
import de.ii.xtraplatform.streams.domain.Reactive.TranformerCustomFuseableOut;
import de.ii.xtraplatform.streams.domain.Reactive.TransformerCustomFuseableIn;
import de.ii.xtraplatform.streams.domain.Reactive.TransformerCustomSource;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class FeatureTokenDecoderSimple<
        T,
        U extends SchemaBase<U>,
        V extends SchemaMappingBase<U>,
        W extends ModifiableContext<U, V>>
    implements TranformerCustomFuseableOut<T, Object, FeatureEventHandlerSimple>,
        TransformerCustomSource<T, Object, FeatureTokenSource>,
        FeatureTokenContextSimple<W> {

  private FeatureEventHandlerSimple<U, V, W> downstream;

  @Override
  public final Class<? extends FeatureEventHandlerSimple> getFusionInterface() {
    return FeatureEventHandlerSimple.class;
  }

  @Override
  public final boolean canFuse(
      TransformerCustomFuseableIn<Object, ?, ?> transformerCustomFuseableIn) {
    boolean isTransformerFuseable =
        TranformerCustomFuseableOut.super.canFuse(transformerCustomFuseableIn);

    if (isTransformerFuseable && transformerCustomFuseableIn instanceof FeatureTokenContext<?>) {
      if (!ModifiableContext.class.isAssignableFrom(
          ((FeatureTokenContext<?>) transformerCustomFuseableIn).getContextInterface())) {
        throw new IllegalStateException(
            "Cannot fuse FeatureTokenTransformer: "
                + ((FeatureTokenContext<?>) transformerCustomFuseableIn).getContextInterface()
                + " does not extend "
                + this.getContextInterface());
      }
    }

    return isTransformerFuseable;
  }

  @Override
  public final void fuse(
      TransformerCustomFuseableIn<Object, ?, ? extends FeatureEventHandlerSimple>
          transformerCustomFuseableIn) {
    if (!canFuse(transformerCustomFuseableIn)) {
      throw new IllegalArgumentException();
    }
    if (Objects.isNull(downstream)) {
      this.downstream = transformerCustomFuseableIn.fuseableSink();

      transformerCustomFuseableIn.afterInit(this::init);
      // init();
    }
  }

  @Override
  public final void init(Consumer<Object> push) {
    if (Objects.isNull(downstream)) {
      this.downstream = (FeatureTokenEmitterSimple<U, V, W>) push::accept;
      init();
    }
  }

  @Override
  public final void onComplete() {
    cleanup();
  }

  @Override
  public FeatureTokenSource getCustomSource(Source<Object> source) {
    return new FeatureTokenSource(source);
  }

  @Override
  public Class<? extends W> getContextInterface() {
    if (downstream instanceof FeatureTokenContextSimple<?>) {
      return ((FeatureTokenContextSimple<W>) downstream).getContextInterface();
    }

    return (Class<? extends W>) GenericContextSimple.class;
  }

  @Override
  public final W createContext() {
    if (downstream instanceof FeatureTokenContextSimple<?>) {
      return ((FeatureTokenContextSimple<W>) downstream).createContext();
    }

    return (W) ModifiableGenericContextSimple.create().setType("default");
  }

  protected final FeatureEventHandlerSimple<U, V, W> getDownstream() {
    return downstream;
  }

  protected void init() {}

  protected void cleanup() {}
}
