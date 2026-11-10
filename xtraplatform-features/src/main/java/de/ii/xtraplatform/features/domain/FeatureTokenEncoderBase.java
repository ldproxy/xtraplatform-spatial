/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class FeatureTokenEncoderBase<
        T extends SchemaBase<T>, U extends SchemaMappingBase<T>, V extends ModifiableContext<T, U>>
    implements FeatureTokenEncoderGeneric<T, U, V> {

  private final FeatureTokenReader<T, U, V> tokenReader;
  private Consumer<byte[]> downstream;
  private Runnable afterInit;

  protected FeatureTokenEncoderBase() {
    this.tokenReader = new FeatureTokenReader<>(this, null);
  }

  @Override
  public final void init(Consumer<byte[]> push) {
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
  public FeatureEventHandler<T, U, V> fuseableSink() {
    return this;
  }

  protected final void push(byte[] t) {
    downstream.accept(t);
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
