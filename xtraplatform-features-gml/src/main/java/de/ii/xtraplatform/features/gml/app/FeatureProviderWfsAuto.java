/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.entities.domain.AutoEntity;
import de.ii.xtraplatform.entities.domain.AutoEntityFactory;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.Metadata;
import de.ii.xtraplatform.features.gml.domain.ConnectionInfoWfsHttp;
import de.ii.xtraplatform.features.gml.domain.FeatureProviderWfsData;
import de.ii.xtraplatform.features.gml.domain.ImmutableFeatureProviderWfsData;
import de.ii.xtraplatform.features.gml.domain.WfsClientBasic;
import de.ii.xtraplatform.features.gml.domain.WfsClientBasicFactory;
import de.ii.xtraplatform.features.gml.infra.WfsSchemaCrawler;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FeatureProviderWfsAuto implements AutoEntityFactory {

  private final WfsClientBasicFactory clientFactory;

  public FeatureProviderWfsAuto(WfsClientBasicFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  @Override
  public <T extends AutoEntity> Map<String, String> check(T entityData) {
    return Map.of();
  }

  @Override
  public <T extends AutoEntity> Map<String, List<String>> analyze(T entityData) {
    if (!(entityData instanceof FeatureProviderWfsData)) {
      return Map.of();
    }

    FeatureProviderWfsData data = (FeatureProviderWfsData) entityData;

    WfsClientBasic wfsClient =
        clientFactory.create(data.getProviderSubType(), data.getId(), data.getConnectionInfo());

    if (wfsClient.getConnectionError().isPresent()) {
      Throwable throwable = wfsClient.getConnectionError().get();
      if (throwable instanceof RuntimeException) {
        throw (RuntimeException) throwable;
      }
    }

    try {
      Optional<Metadata> metadata = wfsClient.getMetadata();

      return metadata.map(Metadata::getFeatureTypes).orElse(ImmutableList.of()).stream()
          .collect(
              Collectors.groupingBy(
                  qn -> Objects.requireNonNullElse(qn.getPrefix(), qn.getNamespaceURI()),
                  LinkedHashMap::new,
                  Collectors.mapping(qn -> qn.getLocalPart(), Collectors.toList())));
    } finally {
      clientFactory.dispose(wfsClient);
    }
  }

  @Override
  public <T extends AutoEntity> T generate(
      T entityData, Map<String, List<String>> types, Consumer<Map<String, List<String>>> tracker) {
    if (!(entityData instanceof FeatureProviderWfsData)) {
      return entityData;
    }

    FeatureProviderWfsData data = (FeatureProviderWfsData) entityData;

    WfsClientBasic wfsClient =
        clientFactory.create(data.getProviderSubType(), data.getId(), data.getConnectionInfo());

    try {
      return (T)
          cleanupAuto(
              cleanupAdditionalInfo(
                  completeConnectionInfoIfNecessary(
                      wfsClient,
                      generateNativeCrsIfNecessary(
                          wfsClient, generateTypesIfNecessary(wfsClient, data, types, tracker)))));

    } finally {
      clientFactory.dispose(wfsClient);
    }
  }

  private FeatureProviderWfsData completeConnectionInfoIfNecessary(
      WfsClientBasic wfsClient, FeatureProviderWfsData data) {
    ConnectionInfoWfsHttp connectionInfo = (ConnectionInfoWfsHttp) data.getConnectionInfo();

    if (connectionInfo.getNamespaces().isEmpty()) {

      WfsSchemaCrawler schemaCrawler = new WfsSchemaCrawler(wfsClient, connectionInfo);

      ConnectionInfoWfsHttp connectionInfoWfsHttp = schemaCrawler.completeConnectionInfo();

      return new ImmutableFeatureProviderWfsData.Builder()
          .from(data)
          .connectionInfo(connectionInfoWfsHttp)
          .build();
    }

    return data;
  }

  private FeatureProviderWfsData generateTypesIfNecessary(
      WfsClientBasic wfsClient,
      FeatureProviderWfsData data,
      Map<String, List<String>> includeTypes,
      Consumer<Map<String, List<String>>> tracker) {
    if (data.getTypes().isEmpty()) {
      ConnectionInfoWfsHttp connectionInfo = (ConnectionInfoWfsHttp) data.getConnectionInfo();

      WfsSchemaCrawler schemaCrawler = new WfsSchemaCrawler(wfsClient, connectionInfo);

      List<FeatureSchema> types = schemaCrawler.parseSchema(includeTypes, tracker);

      ImmutableMap<String, FeatureSchema> typeMap =
          types.stream()
              .map(type -> new AbstractMap.SimpleImmutableEntry<>(type.getName(), type))
              .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

      return new ImmutableFeatureProviderWfsData.Builder().from(data).types(typeMap).build();
    }

    return data;
  }

  private FeatureProviderWfsData generateNativeCrsIfNecessary(
      WfsClientBasic wfsClient, FeatureProviderWfsData data) {
    if (!data.getNativeCrs().isPresent()) {
      ConnectionInfoWfsHttp connectionInfo = (ConnectionInfoWfsHttp) data.getConnectionInfo();

      WfsSchemaCrawler schemaCrawler = new WfsSchemaCrawler(wfsClient, connectionInfo);

      EpsgCrs nativeCrs = schemaCrawler.getNativeCrs().orElse(OgcCrs.CRS84);

      return new ImmutableFeatureProviderWfsData.Builder().from(data).nativeCrs(nativeCrs).build();
    }

    return data;
  }

  private FeatureProviderWfsData cleanupAdditionalInfo(FeatureProviderWfsData data) {
    return new ImmutableFeatureProviderWfsData.Builder()
        .from(data)
        .types(
            data.getTypes().entrySet().stream()
                .map(
                    entry ->
                        new SimpleImmutableEntry<>(
                            entry.getKey(),
                            new ImmutableFeatureSchema.Builder()
                                .from(entry.getValue())
                                .additionalInfo(ImmutableMap.of())
                                .propertyMap(
                                    entry.getValue().getPropertyMap().entrySet().stream()
                                        .map(
                                            entry2 ->
                                                new SimpleImmutableEntry<>(
                                                    entry2.getKey(),
                                                    new ImmutableFeatureSchema.Builder()
                                                        .from(entry2.getValue())
                                                        .additionalInfo(ImmutableMap.of())
                                                        .build()))
                                        .collect(
                                            ImmutableMap.toImmutableMap(
                                                Map.Entry::getKey, Map.Entry::getValue)))
                                .build()))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .build();
  }

  private FeatureProviderWfsData cleanupAuto(FeatureProviderWfsData data) {
    return new ImmutableFeatureProviderWfsData.Builder().from(data).auto(Optional.empty()).build();
  }
}
