/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@JsonDeserialize(builder = ImmutableConnectionInfoGeoParquet.Builder.class)
public interface ConnectionInfoGeoParquet extends ConnectionInfoSql {

  /**
   * @langEn The relative path starting from `/resources/features` to the directory containing the
   *     (Geo)Parquet files (and subdirectories containing (Geo)Parquet files).
   * @langDe Der zu `/resources/features` relative Pfad zum Ordner, in dem die GeoParquet-Dateien
   *     (sowie Unterordner mit (Geo)Parquet-Dateien) liegen.
   */
  @Override
  String getDatabase();

  @DocIgnore
  @JsonIgnore
  @Nullable
  @Override
  PoolSettings getPool();

  @DocIgnore
  @JsonIgnore
  @Override
  @Value.Default
  default String getDialect() {
    return ConnectionInfoSql.super.getDialect();
  }

  @DocIgnore
  @JsonIgnore
  @Override
  Map<String, String> getDriverOptions();
}
