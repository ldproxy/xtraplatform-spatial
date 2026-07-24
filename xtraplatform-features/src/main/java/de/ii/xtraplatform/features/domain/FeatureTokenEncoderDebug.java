/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Strings;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureEventHandlerGeneric.GenericContext;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;

public class FeatureTokenEncoderDebug
    implements FeatureTokenEncoder<ModifiableContext<FeatureSchema, SchemaMapping>>,
        FeatureTokenEmitter2<
            FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>> {

  private Consumer<byte[]> downstream;
  private Runnable afterInit;

  public FeatureTokenEncoderDebug() {}

  @Override
  public final void init(Consumer<byte[]> push) {
    this.downstream = push;
    init();
  }

  @Override
  public void push(Object token) {
    onPush(token);
  }

  @Override
  public final void onPush(Object token) {
    String prefix = token instanceof FeatureTokenType ? "\n" : " ";
    String asString =
        Objects.isNull(token)
            ? "NULL"
            : token instanceof FeatureTokenType
                ? Strings.padEnd(token.toString(), 11, ' ')
                : token instanceof String && ((String) token).isEmpty()
                    ? "EMPTY"
                    : token.toString();

    push((prefix + asString).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public final void onComplete() {
    cleanup();
  }

  @Override
  public FeatureEventHandler<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      fuseableSink() {
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

  @Override
  public Class<? extends ModifiableContext<FeatureSchema, SchemaMapping>> getContextInterface() {
    return GenericContext.class;
  }

  @Override
  public ModifiableContext<FeatureSchema, SchemaMapping> createContext() {
    return ModifiableGenericContext.create();
  }
}
