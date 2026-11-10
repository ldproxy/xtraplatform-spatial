/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.streams.domain.Reactive.TransformerCustomFuseableIn;
import java.util.function.Consumer;

public abstract class FeatureEventEncoder<T>
    implements TransformerCustomFuseableIn<Object, T, FeatureEventConsumer>, FeatureEventConsumer {

  private final FeatureTokenReader<?, ?, ?> tokenReader;
  private Consumer<T> downstream;

  protected FeatureEventEncoder() {
    this.tokenReader = new FeatureTokenReader<>(this);
  }

  @Override
  public final void init(Consumer<T> push) {
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
  public FeatureEventConsumer fuseableSink() {
    return this;
  }

  protected final void push(T t) {
    downstream.accept(t);
  }

  protected void init() {}

  protected void cleanup() {}
}
