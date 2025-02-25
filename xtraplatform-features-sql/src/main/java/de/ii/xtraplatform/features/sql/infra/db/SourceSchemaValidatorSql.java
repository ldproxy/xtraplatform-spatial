/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.entities.domain.ImmutableValidationResult;
import de.ii.xtraplatform.entities.domain.ValidationResult;
import de.ii.xtraplatform.entities.domain.ValidationResult.MODE;
import de.ii.xtraplatform.features.domain.SourceSchemaValidator;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlRelation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.schema.Catalog;
import schemacrawler.schemacrawler.exceptions.SchemaCrawlerException;

public class SourceSchemaValidatorSql implements SourceSchemaValidator<SchemaSql> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceSchemaValidatorSql.class);
  public static final String TABLE_DOES_NOT_EXIST = "%s: table '%s' does not exist";
  public static final String COLUMN_DOES_NOT_EXIST = "%s: column '%s' in table '%s' does not exist";
  public static final String COLUMN_NOT_UNIQUE =
      "%s: column '%s' in table '%s' should not be used as %s, neither a primary key nor a unique constraint can be found";
  public static final String COLUMN_CANNOT_BE_USED_AS =
      "%s: column '%s' in table '%s' cannot be used as %s";

  private final List<String> schemas;
  private Supplier<SqlClient> sqlClient;

  public SourceSchemaValidatorSql(List<String> schemas, Supplier<SqlClient> sqlClient) {
    this.schemas = schemas;
    this.sqlClient = sqlClient;
  }

  @Override
  public ValidationResult validate(String typeName, List<SchemaSql> sourceSchemas, MODE mode) {
    try (SqlSchemaCrawler schemaCrawler = new SqlSchemaCrawler(sqlClient.get().getConnection())) {
      Catalog catalog =
          schemaCrawler.getCatalog(schemas, getAllUsedTables(sourceSchemas), ImmutableList.of());
      SchemaInfo schemaInfo = new SchemaInfo(catalog.getTables());

      return sourceSchemas.stream()
          .flatMap(sourceSchema -> sourceSchema.getAllObjects().stream())
          .map(tableSchema -> validate(typeName, tableSchema, mode, schemaInfo))
          .reduce(ValidationResult.of(), ValidationResult::mergeWith);
    } catch (SchemaCrawlerException | IOException e) {
      throw new IllegalStateException("could not parse schema", e);
    }
  }

  private ValidationResult validate(
      String typeName, SchemaSql tableSchema, MODE mode, SchemaInfo schemaInfo) {
    ImmutableValidationResult.Builder result = ImmutableValidationResult.builder().mode(mode);

    if (!tableSchema.getRelation().isEmpty()) {
      List<SqlRelation> relations = tableSchema.getRelation();

      for (int i = 0; i < relations.size(); i++) {
        SqlRelation relation = relations.get(i);
        String context =
            String.format(
                "Invalid sourcePath '%s' in type '%s'",
                Joiner.on('/').join(relation.asPath()), typeName);

        if (i > 0 && !schemaInfo.tableExists(relation.getSourceContainer())) {
          result.addErrors(
              String.format(TABLE_DOES_NOT_EXIST, context, relation.getSourceContainer()));
        } else if (!schemaInfo.columnExists(
            relation.getSourceField(), relation.getSourceContainer())) {
          result.addErrors(
              String.format(
                  COLUMN_DOES_NOT_EXIST,
                  context,
                  relation.getSourceField(),
                  relation.getSourceContainer()));
        }

        if (!schemaInfo.tableExists(relation.getTargetContainer())) {
          result.addErrors(
              String.format(TABLE_DOES_NOT_EXIST, context, relation.getTargetContainer()));
        } else if (!schemaInfo.columnExists(
            relation.getTargetField(), relation.getTargetContainer())) {
          result.addErrors(
              String.format(
                  COLUMN_DOES_NOT_EXIST,
                  context,
                  relation.getTargetField(),
                  relation.getTargetContainer()));
        }

        if (relation.getJunction().isPresent()
            && !schemaInfo.tableExists(relation.getJunction().get())) {
          result.addErrors(
              String.format(TABLE_DOES_NOT_EXIST, context, relation.getJunction().get()));
        } else {
          if (relation.getJunctionSource().isPresent() && relation.getJunction().isPresent()) {
            if (!schemaInfo.columnExists(
                relation.getJunctionSource().get(), relation.getJunction().get())) {
              result.addErrors(
                  String.format(
                      COLUMN_DOES_NOT_EXIST,
                      context,
                      relation.getJunctionSource().get(),
                      relation.getJunction().get()));
            }
          }

          if (relation.getJunctionTarget().isPresent() && relation.getJunction().isPresent()) {
            if (!schemaInfo.columnExists(
                relation.getJunctionTarget().get(), relation.getJunction().get())) {
              result.addErrors(
                  String.format(
                      COLUMN_DOES_NOT_EXIST,
                      context,
                      relation.getJunctionTarget().get(),
                      relation.getJunction().get()));
            }
          }
        }
      }
    } else if (!schemaInfo.tableExists(tableSchema.getName())) {
      String context = String.format("Invalid sourcePath in type '%s'", typeName);
      result.addErrors(String.format(TABLE_DOES_NOT_EXIST, context, tableSchema.getName()));
    }

    ValidationResult intermediateResult = result.build();
    if (!intermediateResult.isSuccess()) {
      return intermediateResult;
    }

    String context = String.format("Invalid sort key for type '%s'", typeName);
    if (!schemaInfo.columnExists(tableSchema.getSortKey().get(), tableSchema.getName())) {
      result.addErrors(
          String.format(
              COLUMN_DOES_NOT_EXIST,
              context,
              tableSchema.getSortKey().get(),
              tableSchema.getName()));
    } else if (!schemaInfo.isColumnUnique(tableSchema.getSortKey().get(), tableSchema.getName())) {
      result.addStrictErrors(
          String.format(
              COLUMN_NOT_UNIQUE,
              context,
              tableSchema.getSortKey().get(),
              tableSchema.getName(),
              "sort key"));
    }

    tableSchema
        .getProperties()
        .forEach(
            attribute -> {
              if (attribute.isValue() && !attribute.isConstant()) {
                String context2 =
                    String.format(
                        "Invalid sourcePath for property '%s' in type '%s'",
                        attribute.getSourcePath().orElse("???"), typeName);

                if (!schemaInfo.columnExists(attribute.getName(), tableSchema.getName())) {
                  result.addErrors(
                      String.format(
                          COLUMN_DOES_NOT_EXIST,
                          context2,
                          attribute.getName(),
                          tableSchema.getName()));
                } else {
                  if (attribute.isId()
                      && !schemaInfo.isColumnUnique(attribute.getName(), tableSchema.getName())) {
                    String context3 =
                        String.format(
                            "Invalid role ID for property '%s' in type '%s'",
                            attribute.getSourcePath().orElse("???"), typeName);
                    result.addStrictErrors(
                        String.format(
                            COLUMN_NOT_UNIQUE,
                            context3,
                            attribute.getName(),
                            tableSchema.getName(),
                            "feature id"));
                  }

                  if (attribute.isSpatial()
                      && !schemaInfo.isColumnSpatial(tableSchema.getName(), attribute.getName())) {
                    result.addErrors(
                        String.format(
                            COLUMN_CANNOT_BE_USED_AS,
                            context2,
                            attribute.getName(),
                            tableSchema.getName(),
                            "geometry"));
                  }

                  // TODO: strictError on string
                  if (attribute.isTemporal()
                      && !schemaInfo.isColumnTemporal(tableSchema.getName(), attribute.getName())) {
                    result.addErrors(
                        String.format(
                            COLUMN_CANNOT_BE_USED_AS,
                            context2,
                            attribute.getName(),
                            tableSchema.getName(),
                            "datetime"));
                  }
                }
              }
            });

    return result.build();
  }

  private List<String> getAllUsedTables(List<SchemaSql> schemaSql) {
    return schemaSql.stream()
        .flatMap(schemaSql1 -> schemaSql1.getAllObjects().stream())
        .flatMap(schemaSql1 -> getUsedTables(schemaSql1).stream())
        .distinct()
        .collect(Collectors.toList());
  }

  private List<String> getUsedTables(SchemaSql schemaSql) {
    List<String> usedTables = new ArrayList<>();

    usedTables.add(schemaSql.getName());

    for (int i = 0; i < schemaSql.getRelation().size(); i++) {
      SqlRelation relation = schemaSql.getRelation().get(i);
      usedTables.add(relation.getSourceContainer());
      usedTables.add(relation.getTargetContainer());
      if (relation.getJunction().isPresent()) {
        usedTables.add(relation.getJunction().get());
      }
    }

    return usedTables;
  }
}
