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
import java.util.Optional;
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
   * @langEn Only relevant for local (Geo)Parquet files: The relative path starting from
   *     `/resources/features` to the directory containing the (Geo)Parquet files (including
   *     subdirectories).
   * @langDe Nur relevant für lokale (Geo)Parquet-Dateien: Der relative Pfad ausgehend von
   *     `/resources/features` zum Verzeichnis mit den (Geo)Parquet-Dateien (inklusive Unterordner).
   */
  @Override
  String getDatabase();

  /**
   * @langEn Only relevant for S3: The URL of the bucket containing the (Geo)Parquet files. A
   *     subdirectory can be appended to the URL to limit access, e.g.
   *     `s3://bucket-eu-central-1/subdirectory`.
   * @langDe Nur relevant für S3: Die URL des Buckets mit den (Geo)Parquet-Dateien. Um den Zugriff
   *     einzuschränken, können Unterordner an die URL angehängt werden, z.B.
   *     `s3://bucket-eu-central-1/Unterordner`.
   */
  Optional<String> getHost();

  /**
   * @langEn Only relevant for S3: The `access key` required to access the bucket. Must not be set
   *     for public buckets.
   * @langDe Nur relevant für S3: Der `access key` für den Zugriff auf den Bucket. Darf für den
   *     Zugriff auf öffentlichen Buckets nicht gesetzt werden.
   */
  Optional<String> getUser();

  /**
   * @langEn Only relevant for S3: The `secret access key` required to access the bucket. Must not
   *     be set for public buckets.
   * @langDe Nur relevant für S3: Der `secret access key` für den Zugriff auf den Bucket. Darf für
   *     den Zugriff auf öffentlichen Buckets nicht gesetzt werden.
   */
  Optional<String> getPassword();

  /**
   * @langEn This mapping is used to assign table names to (Geo)Parquet files, see
   *     [below](#mapping-of-tables-to-(geo)parquet-files) for details, and to configure S3, see
   *     [further below](#configuration-of-s3).
   * @langDe Dieses Mapping dient sowohl der Zuordnung von Tabellennamen zu (Geo)Parquet-Dateien,
   *     siehe [unten](#zuordnung-von-tabellen-zu-(geo)parquet-dateien) für Details, als auch dem
   *     Konfigurieren von S3, siehe [weiter unten](#konfiguration-von-s3).
   */
  @Override
  Map<String, String> getDriverOptions();

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
}
