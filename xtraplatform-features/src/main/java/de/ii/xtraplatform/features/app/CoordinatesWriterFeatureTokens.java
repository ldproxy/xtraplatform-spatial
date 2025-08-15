/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.geometries.domain.transcode.CoordinatesWriter;
import java.io.IOException;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CoordinatesWriterFeatureTokens
    implements CoordinatesWriter<
        FeatureEventHandler<
            FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>> {

  @Value.Parameter
  public abstract ModifiableContext<FeatureSchema, SchemaMapping> getContext();

  @Override
  public void onStart() throws IOException {
    getDelegate().onArrayStart(getContext());
  }

  @Override
  public void onSeparator() throws IOException {
    getDelegate().onArrayEnd(getContext());
    getDelegate().onArrayStart(getContext());
  }

  @Override
  public void onX(char[] chars, int offset, int length) throws IOException {
    getContext().setValue(String.valueOf(chars, offset, length));
    getDelegate().onValue(getContext());
  }

  @Override
  public void onY(char[] chars, int offset, int length) throws IOException {
    getContext().setValue(String.valueOf(chars, offset, length));
    getDelegate().onValue(getContext());
  }

  @Override
  public void onZ(char[] chars, int offset, int length) throws IOException {
    getContext().setValue(String.valueOf(chars, offset, length));
    getDelegate().onValue(getContext());
  }

  @Override
  public void onEnd() throws IOException {
    getDelegate().onArrayEnd(getContext());
  }
}
