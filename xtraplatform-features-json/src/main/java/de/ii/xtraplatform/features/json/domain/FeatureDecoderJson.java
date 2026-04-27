/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.FeatureDecoder;
import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableSchemaMapping;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import java.util.Objects;
import java.util.Optional;

public class FeatureDecoderJson extends FeatureDecoder<byte[]> implements Decoder.Pipeline {

  private final DecoderJson decoderJson;
  private final FeatureSchema featureSchema;
  private final FeatureQuery featureQuery;

  private ModifiableContext<FeatureSchema, SchemaMapping> context;

  public FeatureDecoderJson(
      FeatureSchema featureSchema,
      FeatureQuery query,
      String type,
      String wrapper,
      Optional<String> nullValue) {
    super();
    Objects.requireNonNull(type);
    Objects.requireNonNull(wrapper);
    this.decoderJson = new DecoderJson(nullValue);
    this.featureSchema = featureSchema;
    this.featureQuery = query;
  }

  @Override
  protected void init() {
    this.context =
        createContext()
            .setType(featureSchema.getName())
            .setMappings(
                ImmutableMap.of(
                    featureSchema.getName(),
                    new ImmutableSchemaMapping.Builder()
                        .targetSchema(featureSchema)
                        .sourcePathTransformer((path, isValue) -> path)
                        .build()))
            .setQuery(featureQuery);
  }

  @Override
  protected void cleanup() {
    try {
      decoderJson.close();
    } catch (Exception e) { // NOPMD AvoidCatchingGenericException
      // ignore
    }
  }

  @Override
  public void onPush(byte[] bytes) {
    decoderJson.decode(bytes, this);
  }

  @Override
  public ModifiableContext<FeatureSchema, SchemaMapping> context() {
    return context;
  }

  @Override
  public FeatureEventHandler<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      downstream() {
    return getDownstream();
  }
}
