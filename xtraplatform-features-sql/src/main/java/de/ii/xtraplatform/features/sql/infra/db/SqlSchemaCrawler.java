/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import de.ii.xtraplatform.features.domain.ImmutableTuple;
import de.ii.xtraplatform.features.domain.Tuple;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import schemacrawler.inclusionrule.InclusionRule;
import schemacrawler.inclusionrule.RegularExpressionExclusionRule;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.inclusionrule.RegularExpressionRule;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.LimitOptionsBuilder;
import schemacrawler.schemacrawler.LoadOptions;
import schemacrawler.schemacrawler.LoadOptionsBuilder;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.schemacrawler.exceptions.SchemaCrawlerException;
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.datasource.DatabaseConnectionSource;

public class SqlSchemaCrawler implements Closeable {

  private final DatabaseConnectionSource connection;

  public SqlSchemaCrawler(Connection connection) {
    this.connection = new SingleDatabaseConnectionSource(connection);
  }

  public Catalog getCatalog(List<String> excludeSchemas, List<String> excludeTables)
      throws SchemaCrawlerException {
    return crawlSchemasAndTables(excludeSchemas, excludeTables);
  }

  public Catalog getCatalog(String schema, String table) throws SchemaCrawlerException {
    return getCatalogAndMatching(
            schema.isEmpty() ? List.of() : List.of(schema), List.of(table), List.of())
        .first();
  }

  public Catalog getCatalog(
      List<String> schemas, List<String> includeTables, List<String> excludeTables)
      throws SchemaCrawlerException {
    return getCatalogAndMatching(schemas, includeTables, excludeTables).first();
  }

  public Tuple<Catalog, List<String>> getCatalogAndMatching(
      List<String> schemas, List<String> includeTables, List<String> excludeTables)
      throws SchemaCrawlerException {
    Catalog catalog = crawlWithDetails(schemas, includeTables, excludeTables);
    List<String> matchingTables =
        catalog.getTables().stream().map(Table::getName).collect(Collectors.toList());

    if (!includeTables.isEmpty()
        && catalog.getTables().stream().anyMatch(table -> table.getTableType().isView())) {
      List<String> additionalTables =
          Stream.concat(
                  includeTables.stream(),
                  catalog.getTables().stream()
                      .filter(table -> table.getTableType().isView())
                      .flatMap(table -> ViewInfo.getOriginalTables(table.getDefinition()).stream()))
              .collect(Collectors.toList());

      if (additionalTables.size() > includeTables.size()) {
        return ImmutableTuple.of(
            crawlWithDetails(schemas, additionalTables, excludeTables), matchingTables);
      }
    }

    return ImmutableTuple.of(catalog, matchingTables);
  }

  private Catalog crawlWithDetails(
      List<String> schemas, List<String> includeTables, List<String> excludeTables)
      throws SchemaCrawlerException {
    String includeSchemas = schemas.stream().distinct().collect(Collectors.joining("|", "(", ")"));

    Collector<CharSequence, ?, String> tableCollector =
        schemas.isEmpty()
            ? Collectors.joining("|", "(", ")")
            : Collectors.joining("|.*\\.", "(.*\\.", ")");
    String includeTablesPattern = includeTables.stream().distinct().collect(tableCollector);
    String excludeTablesPattern = excludeTables.stream().distinct().collect(tableCollector);

    InclusionRule tablesRule =
        includeTables.isEmpty() && !excludeTables.isEmpty()
            ? new RegularExpressionExclusionRule(excludeTablesPattern)
            : !includeTables.isEmpty() && excludeTables.isEmpty()
                ? new RegularExpressionInclusionRule(includeTablesPattern)
                : new RegularExpressionRule(includeTablesPattern, excludeTablesPattern);

    LimitOptionsBuilder limitOptionsBuilder =
        LimitOptionsBuilder.builder()
            .tableTypes("BASE TABLE", "TABLE", "VIEW", "MATERIALIZED VIEW")
            .includeTables(tablesRule);
    if (!schemas.isEmpty()) {
      limitOptionsBuilder.includeSchemas(new RegularExpressionInclusionRule(includeSchemas));
    }

    SchemaCrawlerOptions options =
        SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions()
            .withLimitOptions(limitOptionsBuilder.toOptions())
            .withLoadOptions(fullDetails());

    return SchemaCrawlerUtility.getCatalog(connection, options);
  }

  private Catalog crawlSchemasAndTables(List<String> excludeSchemas, List<String> excludeTables)
      throws SchemaCrawlerException {
    LimitOptionsBuilder limitOptionsBuilder =
        LimitOptionsBuilder.builder()
            .tableTypes("BASE TABLE", "TABLE", "VIEW", "MATERIALIZED VIEW");

    if (!excludeSchemas.isEmpty()) {
      String excludeSchemasString =
          excludeSchemas.stream().distinct().collect(Collectors.joining("|", "(", ")"));
      limitOptionsBuilder.includeSchemas(new RegularExpressionExclusionRule(excludeSchemasString));
    }
    if (!excludeTables.isEmpty()) {
      Collector<CharSequence, ?, String> tableCollector =
          excludeSchemas.isEmpty()
              ? Collectors.joining("|", "(", ")")
              : Collectors.joining("|.*\\.", "(.*\\.", ")");
      String excludeTablesString = excludeTables.stream().distinct().collect(tableCollector);
      limitOptionsBuilder.includeTables(new RegularExpressionExclusionRule(excludeTablesString));
    }

    SchemaCrawlerOptions options =
        SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions()
            .withLimitOptions(limitOptionsBuilder.toOptions())
            .withLoadOptions(onlyTableNames());

    return SchemaCrawlerUtility.getCatalog(connection, options);
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (Exception exception) {
      throw new IOException(exception);
    }
  }

  private static LoadOptions onlyTableNames() {
    return LoadOptionsBuilder.builder()
        .withSchemaInfoLevel(
            SchemaInfoLevelBuilder.builder()
                .setRetrieveTables(true)
                .setRetrieveViewInformation(true) // needed???
                .setRetrieveColumnDataTypes(false)
                .setRetrieveIndexes(false)
                .setRetrieveIndexInformation(false)
                .setRetrieveTableColumns(false)
                .setRetrievePrimaryKeys(false)
                .setRetrieveTableConstraints(false)
                .setRetrieveTableConstraintDefinitions(false)
                .setRetrieveTableConstraintInformation(false)
                .setRetrieveTableDefinitionsInformation(false)
                .setRetrieveUserDefinedColumnDataTypes(false)
                .setRetrieveViewViewTableUsage(false)
                .setRetrieveAdditionalColumnAttributes(false)
                .setRetrieveAdditionalColumnMetadata(false)
                .setRetrieveAdditionalDatabaseInfo(false)
                .setRetrieveAdditionalJdbcDriverInfo(false)
                .setRetrieveAdditionalTableAttributes(false)
                .setRetrieveDatabaseInfo(false)
                .setRetrieveDatabaseUsers(false)
                .setRetrieveForeignKeys(false)
                .setRetrieveRoutineInformation(false)
                .setRetrieveRoutineParameters(false)
                .setRetrieveRoutines(false)
                .setRetrieveSequenceInformation(false)
                .setRetrieveServerInfo(false)
                .setRetrieveSynonymInformation(false)
                .setRetrieveTableColumnPrivileges(false)
                .setRetrieveTablePrivileges(false)
                .setRetrieveTriggerInformation(false)
                .toOptions())
        .toOptions();
  }

  private static LoadOptions fullDetails() {
    return LoadOptionsBuilder.builder()
        .withSchemaInfoLevel(
            SchemaInfoLevelBuilder.builder()
                .setRetrieveColumnDataTypes(true)
                .setRetrieveIndexes(true)
                .setRetrieveIndexInformation(true)
                .setRetrieveTableColumns(true)
                .setRetrievePrimaryKeys(true)
                .setRetrieveTableConstraints(true)
                .setRetrieveTableConstraintDefinitions(true) // needed???
                .setRetrieveTableConstraintInformation(true)
                .setRetrieveTableDefinitionsInformation(true)
                .setRetrieveTables(true)
                .setRetrieveUserDefinedColumnDataTypes(true)
                .setRetrieveViewInformation(true)
                .setRetrieveViewViewTableUsage(true)
                .setRetrieveAdditionalColumnAttributes(false)
                .setRetrieveAdditionalColumnMetadata(false)
                .setRetrieveAdditionalDatabaseInfo(false)
                .setRetrieveAdditionalJdbcDriverInfo(false)
                .setRetrieveAdditionalTableAttributes(false)
                .setRetrieveDatabaseInfo(false)
                .setRetrieveDatabaseUsers(false)
                .setRetrieveForeignKeys(false)
                .setRetrieveRoutineInformation(false)
                .setRetrieveRoutineParameters(false)
                .setRetrieveRoutines(false)
                .setRetrieveSequenceInformation(false)
                .setRetrieveServerInfo(false)
                .setRetrieveSynonymInformation(false)
                .setRetrieveTableColumnPrivileges(false)
                .setRetrieveTablePrivileges(false)
                .setRetrieveTriggerInformation(false)
                .toOptions())
        .toOptions();
  }
}
