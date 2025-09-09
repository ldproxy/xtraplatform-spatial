/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class GeometryDecoderWkt implements Decoder {

  private final de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryDecoderWkt wktDecoder;
  private FeatureEventHandler<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      handler;
  private ModifiableContext<FeatureSchema, SchemaMapping> context;

  public GeometryDecoderWkt() {
    wktDecoder = new de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryDecoderWkt();
  }

  @Override
  public void decode(byte[] data, Pipeline pipeline) {
    if (Objects.isNull(context)) {
      this.handler = pipeline.downstream();
      this.context = pipeline.context();
    }
    try {
      context.setGeometry(wktDecoder.decode(new String(data, StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    handler.onGeometry(context);
  }

  @Override
  public void close() throws Exception {}
}
