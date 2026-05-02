/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableMap;
import dagger.Lazy;
import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.DecoderFactories;
import de.ii.xtraplatform.features.domain.DecoderFactory;
import de.ii.xtraplatform.features.domain.DecoderFactory.FeatureDecoderFactory;
import de.ii.xtraplatform.features.domain.DecoderFactory.GeometryDecoderFactory;
import de.ii.xtraplatform.features.domain.FeatureDecoder;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.MediaType;

@Singleton
@AutoBind
public class DecoderFactoriesImpl implements DecoderFactories {

  private final Lazy<Set<DecoderFactory>> factories;
  private final Lazy<Set<DecoderFactory.GeometryDecoderFactory>> geometryFactories;
  private final Lazy<Set<DecoderFactory.FeatureDecoderFactory>> featureFactories;

  @Inject
  public DecoderFactoriesImpl(
      Lazy<Set<DecoderFactory>> factories,
      Lazy<Set<DecoderFactory.GeometryDecoderFactory>> geometryFactories,
      Lazy<Set<DecoderFactory.FeatureDecoderFactory>> featureFactories) {
    this.factories = factories;
    this.geometryFactories = geometryFactories;
    this.featureFactories = featureFactories;
  }

  @Override
  public Optional<Decoder> createDecoder(MediaType mediaType) {
    return factories.get().stream()
        .filter(factory -> factory.getMediaType().equals(mediaType))
        .map(DecoderFactory::createDecoder)
        .findFirst();
  }

  @Override
  public Optional<FeatureDecoder<byte[]>> createFeatureDecoder(MediaType mediaType) {
    return featureFactories.get().stream()
        .filter(factory -> factory.getMediaType().equals(mediaType))
        .map(FeatureDecoderFactory::createFeatureDecoder)
        .findFirst();
  }

  @Override
  public Optional<Decoder> createGeometryDecoder(MediaType mediaType) {
    return geometryFactories.get().stream()
        .filter(factory -> factory.getGeometryMediaType().equals(mediaType))
        .map(GeometryDecoderFactory::createGeometryDecoder)
        .findFirst();
  }

  @Override
  public Map<String, DecoderFactory> getConnectorDecoders() {
    return factories.get().stream()
        .filter(factory -> factory.getConnectorString().isPresent())
        .map(factory -> Map.entry(factory.getConnectorString().get(), factory))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
