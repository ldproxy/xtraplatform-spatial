/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@JsonDeserialize(builder = ImmutableSqlPathDefaults.Builder.class)
public interface SqlPathDefaults {

  /**
   * @langEn The default column that is used for join analysis if no differing primary key is set in
   *     the [sourcePath](#path-syntax). Only relevant for CRUD.
   * @langDe Die Standard-Spalte die zur Analyse von Joins verwendet wird, wenn keine abweichende
   *     Spalte in `sourcePath` gesetzt wird. Nur relevant für CRUD.
   * @default `id`
   */
  @Value.Default
  default String getPrimaryKey() {
    return "id";
  }

  /**
   * @langEn The default column that is used to sort rows if no differing sort key is set in the
   *     [sourcePath](#path-syntax).
   * @langDe Die Standard-Spalte die zur Sortierung von Reihen verwendet wird, wenn keine
   *     abweichende Spalte in `sourcePath` gesetzt wird. Es wird empfohlen, dass als Datentyp eine
   *     Ganzzahl verwendet wird.
   * @default `id`
   */
  @Value.Default
  default String getSortKey() {
    return "id";
  }

  /**
   * @since v3.3
   * @langEn The default schema that is applied to tables without prefix in
   *     [sourcePaths](#path-syntax).
   * @langDe Das Standard-Schema das Tabellen ohne Präfix in `sourcePath` vorangestellt wird.
   * @default null
   */
  Optional<String> getSchema();

  @DocIgnore
  Optional<String> getJunctionTablePattern();
}
