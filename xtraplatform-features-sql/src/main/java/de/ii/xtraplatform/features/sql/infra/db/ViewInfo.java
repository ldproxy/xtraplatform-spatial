/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.ImmutableTuple;
import de.ii.xtraplatform.features.domain.Tuple;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class ViewInfo {

  public static List<String> getOriginalTables(String viewDefinition) {
    if (Objects.isNull(viewDefinition)) {
      return ImmutableList.of();
    }
    try {
      PlainSelect select = parse(viewDefinition);

      TablesNamesFinder<Void> tablesNamesFinder =
          new TablesNamesFinder<Void>() {
            @Override
            protected String extractTableName(Table table) {
              return table.getName();
            }
          };

      Statement statementVisitor =
          new Statement() {
            @Override
            public <T, S> T accept(StatementVisitor<T> statementVisitor, S s) {
              return select.accept(statementVisitor, null);
            }
          };

      return tablesNamesFinder.getTableList(statementVisitor);

    } catch (JSQLParserException | ParseException e) {
      // ignore
      boolean br = true;
    }

    return ImmutableList.of();
  }

  public static Optional<Tuple<String, String>> getOriginalTableAndColumn(
      String viewDefinition, String columnName) {
    if (Objects.isNull(viewDefinition)) {
      return Optional.empty();
    }
    try {
      PlainSelect select = parse(viewDefinition);

      return getOriginalTableAndColumn(select, columnName)
          .map(tableAndColumn -> resolveTableAlias(select, tableAndColumn));

    } catch (JSQLParserException | ParseException e) {
      // ignore
      boolean br = true;
    }

    return Optional.empty();
  }

  private static Optional<Tuple<String, String>> getOriginalTableAndColumn(
      PlainSelect select, String columnName) {
    ImmutableTuple.Builder<String, String> builder =
        ImmutableTuple.<String, String>builder().second(columnName);

    for (SelectItem<? extends Expression> selectItem : select.getSelectItems()) {
      selectItem.accept(
          new SelectItemVisitorAdapter<Void>() {
            @Override
            public void visit(SelectItem<? extends Expression> item) {
              if (item.getExpression() instanceof Column) {
                Column column = (Column) item.getExpression();

                if (Objects.nonNull(item.getAlias())
                    && Objects.equals(item.getAlias().getName(), columnName)) {
                  builder.first(column.getTable().getName()).second(column.getColumnName());
                } else if (Objects.equals(column.getColumnName(), columnName)) {
                  builder.first(column.getTable().getName());
                }
              }
            }
          },
          null);
    }
    try {
      return Optional.of(builder.build());
    } catch (Throwable e) {
      // column not found
    }

    return Optional.empty();
  }

  private static Tuple<String, String> resolveTableAlias(
      PlainSelect select, Tuple<String, String> tableAndColumn) {
    ImmutableTuple.Builder<String, String> builder =
        ImmutableTuple.<String, String>builder().from(tableAndColumn);

    select
        .getFromItem()
        .accept(
            new FromItemVisitorAdapter<Void>() {
              @Override
              public void visit(Table tableName) {
                if (Objects.nonNull(tableName.getAlias())
                    && Objects.equals(tableName.getAlias().getName(), tableAndColumn.first())) {
                  builder.first(tableName.getName());
                }
              }

              @Override
              public void visit(ParenthesedFromItem subjoin) {
                subjoin.getFromItem().accept(this);
                subjoin.getJoins().forEach(join -> join.getRightItem().accept(this));
              }
            });

    return builder.build();
  }

  private static PlainSelect parse(String select) throws JSQLParserException, ParseException {
    net.sf.jsqlparser.statement.Statement parsed =
        CCJSqlParserUtil.parse(select, ccjSqlParser -> ccjSqlParser.setErrorRecovery(true));

    Select statement;
    if (parsed instanceof Select) {
      statement = (Select) parsed;
    } else if (parsed instanceof CreateView) {
      statement = ((CreateView) parsed).getSelect();
    } else {
      throw new JSQLParserException("View definition is not a SELECT statement");
    }

    PlainSelect selectBody = (PlainSelect) statement.getSelectBody();

    if (Objects.isNull(selectBody.getFromItem())) {
      CCJSqlParser from =
          new CCJSqlParser(new StringProvider(select.substring(select.lastIndexOf("FROM") + 5)));

      selectBody.setFromItem(from.FromItem());
    }

    return selectBody;
  }
}
