/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class FeatureTokenEncoderBaseSimple<T, U, V extends ModifiableContext<T, U>, W>
    implements FeatureTokenEncoderGenericSimple<T, U, V, W> {

  private final FeatureTokenReaderSimple<T, U, V> tokenReader;
  private Consumer<W> downstream;
  private Runnable afterInit;

  protected FeatureTokenEncoderBaseSimple() {
    this.tokenReader = new FeatureTokenReaderSimple<>(this, null);
  }

  @Override
  public final void init(Consumer<W> push) {
    this.downstream = push;
    init();
  }

  @Override
  public final void onPush(Object token) {
    tokenReader.onToken(token);
  }

  @Override
  public final void onComplete() {
    cleanup();
  }

  @Override
  public FeatureEventHandlerSimple<T, U, V> fuseableSink() {
    return this;
  }

  protected final void push(W w) {
    downstream.accept(w);
  }

  @Override
  public void afterInit(Runnable runnable) {
    this.afterInit = runnable;
  }

  protected void init() {
    if (Objects.nonNull(afterInit)) {
      afterInit.run();
    }
  }

  protected void cleanup() {}
}
