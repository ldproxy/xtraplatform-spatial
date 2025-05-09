/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.SqlRelation;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class AliasGenerator {

  public static List<String> getAliases(List<SchemaSql> parents, SchemaSql schema) {
    char alias = 'A';

    if (parents.isEmpty() && schema.getRelation().isEmpty()) {
      return ImmutableList.of(String.valueOf(alias));
    }

    ImmutableList.Builder<String> aliases = new ImmutableList.Builder<>();

    List<SqlRelation> relations =
        Stream.concat(
                parents.stream().flatMap(parent -> parent.getRelation().stream()),
                schema.getRelation().stream())
            .collect(Collectors.toList());

    for (SqlRelation relation : relations) {
      aliases.add(String.valueOf(alias++));
      if (relation.isM2N()) {
        aliases.add(String.valueOf(alias++));
      }
    }

    aliases.add(String.valueOf(alias++));

    return aliases.build();
  }

  public static List<String> getAliases(SchemaSql schema) {
    char alias = 'A';

    if (schema.getParentPath().isEmpty()) {
      return ImmutableList.of(String.valueOf(alias));
    }

    ImmutableList.Builder<String> aliases = new ImmutableList.Builder<>();

    for (String relation : schema.getParentPath()) {
      aliases.add(String.valueOf(alias++));
    }

    aliases.add(String.valueOf(alias++));

    return aliases.build();
  }

  public static List<String> getAliases(SqlQuerySchema schema) {
    char alias = 'A';

    if (schema.getRelations().isEmpty()) {
      return ImmutableList.of(String.valueOf(alias));
    }

    ImmutableList.Builder<String> aliases = new ImmutableList.Builder<>();

    for (SqlQueryJoin relation : schema.getRelations()) {
      aliases.add(String.valueOf(alias++));
    }

    aliases.add(String.valueOf(alias++));

    return aliases.build();
  }

  public static List<String> getAliases(List<?> tablePath) {
    char alias = 'A';

    ImmutableList.Builder<String> aliases = new ImmutableList.Builder<>();

    for (Object table : tablePath) {
      aliases.add(String.valueOf(alias++));
    }

    return aliases.build();
  }

  public static List<String> getAliases(SchemaSql schema, int level) {
    if (level > 0) {
      String prefix = IntStream.range(0, level).mapToObj(i -> "A").collect(Collectors.joining());

      return getAliases(schema).stream().map(s -> prefix + s).collect(Collectors.toList());
    }

    return getAliases(schema);
  }

  public static List<String> getAliases(SqlQuerySchema schema, int level) {
    if (level > 0) {
      String prefix = IntStream.range(0, level).mapToObj(i -> "A").collect(Collectors.joining());

      return getAliases(schema).stream().map(s -> prefix + s).collect(Collectors.toList());
    }

    return getAliases(schema);
  }

  public static List<String> getAliases(List<?> tablePath, int level) {
    if (level > 0) {
      String prefix = IntStream.range(0, level).mapToObj(i -> "A").collect(Collectors.joining());

      return getAliases(tablePath).stream().map(s -> prefix + s).collect(Collectors.toList());
    }

    return getAliases(tablePath);
  }

  public static List<String> getAliases(List<SchemaSql> parents, SchemaSql schema, int level) {
    if (level > 0) {
      String prefix = IntStream.range(0, level).mapToObj(i -> "A").collect(Collectors.joining());

      return getAliases(parents, schema).stream().map(s -> prefix + s).collect(Collectors.toList());
    }

    return getAliases(schema);
  }
}
