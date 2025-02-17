/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.oracle.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import java.util.List;
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
@JsonDeserialize(builder = ImmutableConnectionInfoOracle.Builder.class)
public interface ConnectionInfoOracle extends ConnectionInfoSql {

  /**
   * @langEn The name of the database.
   * @langDe Der Name der Datenbank.
   */
  @Override
  String getDatabase();

  /**
   * @langEn The database host. To use a non-default port, add it to the host separated by `:`, e.g.
   *     `db:1523`.
   * @langDe Der Datenbankhost. Wird ein anderer Port als der Standardport verwendet, ist dieser
   *     durch einen Doppelpunkt getrennt anzugeben, z.B. `db:1523`.
   */
  @Override
  Optional<String> getHost();

  /**
   * @langEn The user name.
   * @langDe Der Benutzername.
   */
  @Override
  Optional<String> getUser();

  /**
   * @langEn The base64 encoded password of the user.
   * @langDe Das mit base64 kodierte Passwort des Benutzers.
   */
  @Override
  Optional<String> getPassword();

  /**
   * @langEn The names of database schemas that should be used.
   * @langDe Die Namen der Schemas in der Datenbank, auf die zugegriffen werden soll.
   * @default []
   */
  @Override
  List<String> getSchemas();

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
