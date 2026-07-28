/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ReverseSchemaDeriver;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.sql.SqlPath;
import de.ii.xtraplatform.features.sql.domain.ImmutableSchemaSql.Builder;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.features.sql.domain.SqlPathParser;
import de.ii.xtraplatform.features.sql.domain.SqlRelation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationSchemaDeriver implements ReverseSchemaDeriver<SchemaSql> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutationSchemaDeriver.class);

  private static final Joiner JOINER = Joiner.on('/').skipNulls();
  private static final String IGNORE = "__IGNORE__";

  private final PathParserSql pathParser;
  private final SqlPathParser pathParser3;
  private int ignoreCounter;

  public MutationSchemaDeriver(PathParserSql pathParser, SqlPathParser pathParser3) {
    this.pathParser = pathParser;
    this.pathParser3 = pathParser3;
    this.ignoreCounter = 0;
  }

  @Override
  public SchemaSql create(List<String> path, FeatureSchema targetSchema) {
    List<String> path2 =
        path.stream()
            .map(path1 -> targetSchema.isValue() ? path1 : pathParser3.tablePathWithDefaults(path1))
            .collect(Collectors.toList());

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("OLD {} {}", targetSchema.isObject() ? "OBJECT" : "VALUE", path2);
    }

    return new Builder()
        .name(path2.get(path2.size() - 1))
        .parentPath(path2.subList(0, path2.size() - 1))
        .type(targetSchema.getType())
        .valueType(targetSchema.getValueType())
        .geometryType(targetSchema.getGeometryType())
        .role(targetSchema.getRole())
        // .target(targetSchema.getFullPath())
        .sourcePath(targetSchema.getName())
        .build();
  }

  @Override
  public SchemaSql create(String path, FeatureSchema targetSchema) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("NEW {} {}", targetSchema.isObject() ? "OBJECT" : "VALUE", path);
    }

    if (targetSchema.isValue()) {
      pathParser3.parseColumnPath(path);
    }

    return null;
  }

  @Override
  public List<SchemaSql> createParents(
      String parentParentPath, SchemaSql child, Map<List<String>, SchemaSql> objectCache) {

    String path = JOINER.join(child.getFullPath());
    if (!shouldIgnore(parentParentPath)) {
      path = parentParentPath + "/" + path;
    }

    Optional<SqlPath> sqlPath = pathParser.parse(path, child.isValue());

    // TODO: column?
    if (!sqlPath.isPresent()) {
      throw new IllegalArgumentException("Parse error for SQL path: " + path);
    }

    List<SqlRelation> relations =
        ImmutableList
            .of(); // TODO hasRelation ? pathParser.toRelations(tablePathAsList, ImmutableMap.of())
    // : ImmutableList.of();

    SchemaSql currentChild = child;

    List<SchemaSql> parents = new ArrayList<>();

    for (int i = relations.size() - 1; i >= 0; i--) {
      SqlRelation relation = relations.get(i);

      SchemaBase.Type type =
          relation.isOne2One() ? SchemaBase.Type.OBJECT : SchemaBase.Type.OBJECT_ARRAY;

      boolean replace = currentChild.isObject() && parents.isEmpty();

      SchemaSql parent =
          objectCache.computeIfAbsent(
              relation.asPath(), ignore -> createParent(relation, type, child, replace));

      currentChild = replace ? parent : addChild(parent, currentChild);

      parents.add(0, currentChild);
    }

    return parents;
  }

  private SchemaSql createParent(
      SqlRelation relation, SchemaBase.Type type, SchemaSql child, boolean replace) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("OBJECT {}", relation);
    }

    // List<String> targetPath = (List<String>) child.getTarget()
    //                                              .get();
    Builder builder =
        new Builder()
            .name(relation.getTargetContainer())
            .type(type)
            .addRelation(relation)
            .parentPath(child.getParentPath().subList(0, child.getParentPath().size() - 1));
    // .target(targetPath.subList(0, targetPath.size() - 1))
    // .sourcePath("");

    if (replace) {
      builder
          .parentPath(child.getParentPath().subList(0, child.getParentPath().size() - 1))
          .properties(child.getProperties())
          .sourcePath(child.getSourcePath()); // .target(child.getTarget());
    }

    return builder.build();
  }

  private SchemaSql addChild(SchemaSql parent, SchemaSql child) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("CHILD {} {}", parent.getName(), child.getName());
    }

    SchemaSql toAdd = child;

    if (child.getValueType().isPresent()) {
      toAdd =
          new Builder()
              .from(child)
              .type(child.getValueType().get())
              .valueType(Optional.empty())
              .build();
    }

    return new Builder().from(parent).addProperties(toAdd).build();
  }

  @Override
  public SchemaSql addChildren(SchemaSql parent, List<SchemaSql> children) {
    return new Builder().from(parent).addAllProperties(children).build();
  }

  @Override
  public SchemaSql prependToParentPath(List<String> path, SchemaSql schema) {
    if (shouldIgnore(path)) {
      return schema;
    }

    return new Builder()
        .from(schema)
        .parentPath(path)
        .addAllParentPath(schema.getParentPath())
        .properties(
            schema.getProperties().stream()
                .map(prop -> prependToParentPath(path, prop))
                .collect(Collectors.toList()))
        .build();
  }

  @Override
  public SchemaSql prependToSourcePath(String parentSourcePath, SchemaSql schema) {
    return new Builder()
        .from(schema)
        .sourcePath(JOINER.join(parentSourcePath, schema.getSourcePath().orElse(null)))
        .build();
  }

  @Override
  public String ignore() {
    String result = IGNORE + ignoreCounter;
    ignoreCounter++;
    return result;
  }

  private boolean shouldIgnore(String path) {
    return path.startsWith(IGNORE);
  }

  @Override
  public boolean shouldIgnore(List<String> path) {
    return path.isEmpty() || shouldIgnore(path.get(0));
  }
}
