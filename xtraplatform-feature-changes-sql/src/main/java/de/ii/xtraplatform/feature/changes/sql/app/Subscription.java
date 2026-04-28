/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.changes.sql.app;

import com.google.common.base.Strings;
import de.ii.xtraplatform.base.domain.util.Tuple;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;

@Value.Immutable
interface Subscription {

  int getIndex();

  String getType();

  String getTable();

  String getIdColumn();

  Optional<String> getGeometryColumn();

  Optional<Tuple<String, String>> getIntervalColumns();

  Optional<String> getInstantColumn();

  Optional<Connection> getCurrentConnection();

  Supplier<Connection> getConnectionFactory();

  Function<Connection, List<String>> getNotificationPoller();

  @Value.Derived
  default String getLabel() {
    return String.format("%s_%s", getType(), getTable());
  }

  @Value.Derived
  default String getChannel() {
    return String.format(
        "%s_%s",
        getLabel().substring(0, Math.min(48, getLabel().length())),
        Strings.padStart(String.valueOf(getIndex()), 3, '0'));
  }

  @Value.Lazy
  default Connection getConnection() {
    return getCurrentConnection().orElseThrow(() -> new IllegalStateException("Not connected"));
  }

  default Subscription connect() {
    return ((ImmutableSubscription) this).withCurrentConnection(getConnectionFactory().get());
  }

  default boolean isConnected() {
    try {
      return getCurrentConnection().isPresent() && getCurrentConnection().get().isValid(1);
    } catch (SQLException e) {
      // continue
    }
    return false;
  }

  default List<String> pollNotifications() {
    return getNotificationPoller().apply(getConnection());
  }

  @Value.Derived
  default String getCreateFunction() {
    String delete =
        getBlock(
            true,
            getChannel(),
            getIdColumn(),
            getInstantColumn().or(() -> getIntervalColumns().map(Tuple::first)),
            getInstantColumn().or(() -> getIntervalColumns().map(Tuple::second)),
            getGeometryColumn());
    String insertOrUpdate =
        getBlock(
            false,
            getChannel(),
            getIdColumn(),
            getInstantColumn().or(() -> getIntervalColumns().map(Tuple::first)),
            getInstantColumn().or(() -> getIntervalColumns().map(Tuple::second)),
            getGeometryColumn());

    String template =
        "CREATE OR REPLACE FUNCTION pg_temp.%1$s_trigger()\n"
            + " RETURNS trigger\n"
            + " LANGUAGE plpgsql\n"
            + "AS $function$\n"
            + "   DECLARE\n"
            + "      bbox box2d;\n"
            + "   BEGIN\n"
            + "        IF (TG_OP = 'DELETE') THEN\n"
            + "            %2$s"
            + "        ELSE\n"
            + "            %3$s"
            + "        END IF;\n"
            + "    END;\n"
            + "$function$\n"
            + ";";

    return String.format(template, getChannel(), delete, insertOrUpdate);
  }

  private static String getBlock(
      boolean old,
      String channel,
      String idColumn,
      Optional<String> startColumn,
      Optional<String> endColumn,
      Optional<String> geometryColumn) {
    String prefix = old ? "OLD" : "NEW";
    String id = String.format("%s.%s", prefix, idColumn);
    String temporalStart =
        startColumn.isPresent()
            ? String.format(
                "COALESCE(to_char(%s.%s, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),'NULL')",
                prefix, startColumn.get())
            : "''";
    String temporalEnd =
        endColumn.isPresent()
            ? String.format(
                "COALESCE(to_char(%s.%s, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),'NULL')",
                prefix, endColumn.get())
            : "''";
    String spatial =
        geometryColumn.isPresent()
            ? "st_xmin(bbox), st_ymin(bbox), st_xmax(bbox), st_ymax(bbox)"
            : "'', '', '', ''";
    String bbox =
        geometryColumn.isPresent()
            ? String.format(
                "bbox = box2d(ST_Transform(%s.%s,4326));\n", prefix, geometryColumn.get())
            : "";

    return String.format(
        "%6$s"
            + "            PERFORM pg_notify('%1$s', concat_ws(',', TG_OP, %2$s, %3$s, %4$s, %5$s));\n"
            + "            RETURN %7$s;\n",
        channel, id, temporalStart, temporalEnd, spatial, bbox, prefix);
  }

  @Value.Derived
  default String getCreateTrigger() {
    String template =
        "DROP TRIGGER IF EXISTS %1$s_trigger ON %2$s;\n"
            + "CREATE TRIGGER %1$s_trigger\n"
            + "AFTER INSERT OR UPDATE OR DELETE ON %2$s\n"
            + "    FOR EACH ROW EXECUTE PROCEDURE pg_temp.%1$s_trigger();";

    return String.format(template, getChannel(), getTable());
  }

  @Value.Derived
  default String getListen() {
    return String.format("LISTEN %s;", getChannel());
  }
}
