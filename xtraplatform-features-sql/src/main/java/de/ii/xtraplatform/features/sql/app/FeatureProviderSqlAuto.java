/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.EpsgCrs.Force;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.entities.domain.AutoEntity;
import de.ii.xtraplatform.entities.domain.AutoEntityFactory;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.ImmutableConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.ImmutableFeatureProviderSqlData.Builder;
import de.ii.xtraplatform.features.sql.domain.ImmutablePoolSettings;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasic;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasicFactory;
import de.ii.xtraplatform.features.sql.infra.db.SchemaGeneratorSql;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

public class FeatureProviderSqlAuto implements AutoEntityFactory {

  private final SqlClientBasicFactory sqlClientBasicFactory;

  public FeatureProviderSqlAuto(SqlClientBasicFactory sqlClientBasicFactory) {
    this.sqlClientBasicFactory = sqlClientBasicFactory;
  }

  @Override
  public <T extends AutoEntity> Map<String, String> check(T entityData) {
    return Map.of();
  }

  @Override
  public <T extends AutoEntity> Map<String, List<String>> analyze(T entityData) {
    if (!(entityData instanceof FeatureProviderSqlData)) {
      return Map.of();
    }

    FeatureProviderSqlData data = (FeatureProviderSqlData) entityData;

    SqlClientBasic sqlClientBasic =
        sqlClientBasicFactory.create(
            data.getProviderSubType(),
            data.getId(),
            getConnectionInfoWith4Connections(data.getConnectionInfo()));

    try (SchemaGeneratorSql schemaGeneratorSql = new SchemaGeneratorSql(sqlClientBasic)) {
      return schemaGeneratorSql.analyze();
    } catch (IOException e) {
      return Map.of();
    } finally {
      disposeQuietly(sqlClientBasic);
    }
  }

  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private void disposeQuietly(SqlClientBasic sqlClientBasic) {
    try {
      sqlClientBasicFactory.dispose(sqlClientBasic);
    } catch (RuntimeException e) {
      // ignore
    }
  }

  @Override
  public <T extends AutoEntity> T generate(
      T entityData, Map<String, List<String>> types, Consumer<Map<String, List<String>>> tracker) {
    if (!(entityData instanceof FeatureProviderSqlData)) {
      return entityData;
    }

    return (T)
        cleanupAuto(
            cleanupAdditionalInfo(
                generateNativeCrsIfNecessary(
                    generateTypesIfNecessary(
                        (FeatureProviderSqlData) entityData, types, tracker))));
  }

  private FeatureProviderSqlData generateTypesIfNecessary(
      FeatureProviderSqlData data,
      Map<String, List<String>> types,
      Consumer<Map<String, List<String>>> tracker) {
    if (data.getTypes().isEmpty()) {
      SqlClientBasic sqlClientBasic =
          sqlClientBasicFactory.create(
              data.getProviderSubType(),
              data.getId(),
              getConnectionInfoWith4Connections(data.getConnectionInfo()));

      try (SchemaGeneratorSql schemaGeneratorSql = new SchemaGeneratorSql(sqlClientBasic)) {
        List<FeatureSchema> featureSchemas = schemaGeneratorSql.generate(types, tracker);

        Map<String, Integer> idCounter = new LinkedHashMap<>();
        featureSchemas.forEach(
            featureSchema ->
                featureSchema.getProperties().stream()
                    .filter(FeatureSchema::isId)
                    .map(FeatureSchema::getSourcePath)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst()
                    .ifPresent(
                        path -> {
                          if (!idCounter.containsKey(path)) {
                            idCounter.put(path, 0);
                          }
                          idCounter.put(path, idCounter.get(path) + 1);
                        }));

        String mostOftenUsedId =
            idCounter.entrySet().stream()
                .max(Comparator.comparingInt(Entry::getValue))
                .map(Entry::getKey)
                .orElse("id");

        Map<String, FeatureSchema> typeMap =
            featureSchemas.stream()
                .map(
                    type -> {
                      Optional<String> differingSortKey =
                          type.getIdProperty()
                              .filter(
                                  idProperty ->
                                      !Objects.equals(idProperty.getName(), mostOftenUsedId))
                              .map(FeatureSchema::getName);

                      if (differingSortKey.isPresent() && type.getSourcePath().isPresent()) {
                        return new ImmutableFeatureSchema.Builder()
                            .from(type)
                            .sourcePath(
                                String.format(
                                    "%s{sortKey=%s}",
                                    type.getSourcePath().get(), differingSortKey.get()))
                            .build();
                      }

                      return type;
                    })
                .map(type -> new AbstractMap.SimpleImmutableEntry<>(type.getName(), type))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        Builder builder = new Builder().from(data).types(typeMap);

        if (!Objects.equals(mostOftenUsedId, "id")) {
          // LOGGER.debug("CHANGING defaultSortKey to {}", mostOftenUsedId);
          builder.sourcePathDefaultsBuilder().primaryKey(mostOftenUsedId).sortKey(mostOftenUsedId);
        }

        return builder.build();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      } finally {
        disposeQuietly(sqlClientBasic);
      }
    }

    return data;
  }

  private FeatureProviderSqlData generateNativeCrsIfNecessary(FeatureProviderSqlData data) {
    if (data.getNativeCrs().isEmpty()) {
      EpsgCrs nativeCrs =
          data.getTypes().values().stream()
              .flatMap(type -> type.getProperties().stream())
              .filter(
                  property ->
                      property.isSpatial() && property.getAdditionalInfo().containsKey("crs"))
              .findFirst()
              .map(
                  property -> {
                    EpsgCrs crs = EpsgCrs.fromString(property.getAdditionalInfo().get("crs"));
                    Force force = Force.valueOf(property.getAdditionalInfo().get("force"));
                    if (force != crs.getForceAxisOrder()) {
                      crs = EpsgCrs.of(crs.getCode(), force);
                    }
                    return crs;
                  })
              .orElseGet(() -> OgcCrs.CRS84);

      return new Builder().from(data).nativeCrs(nativeCrs).build();
    }

    return data;
  }

  private FeatureProviderSqlData cleanupAuto(FeatureProviderSqlData data) {
    return new Builder().from(data).auto(Optional.empty()).build();
  }

  private FeatureProviderSqlData cleanupAdditionalInfo(FeatureProviderSqlData data) {
    return new Builder()
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

  private ConnectionInfoSql getConnectionInfoWith4Connections(ConnectionInfoSql connectionInfo) {
    if (Objects.isNull(connectionInfo.getPool().getMaxConnections())
        || connectionInfo.getPool().getMaxConnections() <= 0) {
      return new ImmutableConnectionInfoSql.Builder()
          .from(connectionInfo)
          .pool(
              new ImmutablePoolSettings.Builder()
                  .from(connectionInfo.getPool())
                  .maxConnections(4)
                  .build())
          .build();
    }

    return connectionInfo;
  }
}
