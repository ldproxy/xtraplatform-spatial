/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.routes.sql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.crs.domain.CoordinateTuple;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.FeatureQueriesExtension;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlConnector;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt;
import de.ii.xtraplatform.routes.sql.domain.RouteQuery;
import de.ii.xtraplatform.routes.sql.domain.RoutesConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @title Routing
 * @langEn Support for [pgRouting](https://pgrouting.org) to compute routes.
 * @langDe Unterstützung für [pgRouting](https://pgrouting.org) zur Berechnung von Routen.
 * @scopeEn pgRouting can be used to compute routes on a network dataset. This feature provider
 *     extension allows to provide the edges / segments of a route as a query response. The route
 *     segment schema is specified as a feature type.
 *     <p>The route segment feature type has to meet the following conditions:
 *     <p><code>
 *  * - Each feature is a route segment / edge of the route. The edges are returned in sequence from start to end.
 *  * - The name of the feature table is "\_route\_".
 *  * - The feature type has an INTEGER property `node` that identifies the node at the start of the route segment. The value is either the same as `data/source` (traveling in positive edge direction) or as `data/target` (traveling in negative edge direction).
 *  * - The feature type has a FLOAT property `cost` that states the cost of the edge in the direction of travel.
 *  * - The feature type has an OBJECT property `data`.
 *  * - The `data` object has a INTEGER property `source` with the node identifier at the start of edge.
 *  * - The `data` object has a INTEGER property `target` with the node identifier at the end of edge.
 *  * - The `data` object has a FLOAT property `length_m` with the length of the edge in meter.
 *  * - Das `data` object has a GEOMETRY property with a LINE_STRING geometry.
 *  * - The `data` object can have a FLOAT property `duration_forward_s` with the duration for traveling the edge in positive direction.
 *  * - The `data` object can have a FLOAT property `duration_backward_s` with the duration for traveling the edge in negative direction.
 *  * - The `data` object can have a FLOAT property `maxHeight_m` with the maximum vehicle height in meter for the edge.
 *  * - The `data` object can have a FLOAT property `maxWeight_m` with the maximum vehicle weight in metric tons for the edge.
 *  * - The `data` object can have a FLOAT property `maxspeed_forward` with the speed limit on the edge in positive direction.
 *  * - The `data` object can have a FLOAT property `maxspeed_backward` with the speed limit on the edge in negative direction.
 *  *     </code>
 * @scopeDe pgRouting kann zur Berechnung von Routen auf Grundlage eines Knoten-Kanten-Datensatzes
 *     verwendet werden. Diese Feature-Provider-Erweiterung erlaubt, die Kanten/Segmente einer Route
 *     als Antwort auf eine Abfrage bereitzustellen. Das Schema der Routensegmente wird als
 *     Objektart spezifiziert.
 *     <p>Das Schema der Routensegmente muss die folgenden Bedingungen erfüllen:
 *     <p><code>
 * - Jedes Feature ist ein Routensegment / eine Kante der Route. Die Kanten werden der Reihe nach von Anfang bis Ende zurückgegeben.
 * - Der Name der Feature-Tabelle ist "\_route\_".
 * - Die Objektart hat eine INTEGER-Eigenschaft `node`, die den Knoten am Anfang des Routensegments identifiziert. Der Wert ist entweder gleich `data/source` (Route in positiver Richtung der Kante) oder gleich `data/target` (Route in negativer Richtung der Kante).
 * - Die Objektart hat eine FLOAT-Eigenschaft `cost`, die die Kosten der Kante in Fahrtrichtung angibt.
 * - Die Objektart hat eine OBJECT-Eigenschaft `data`.
 * - Das `data`-Objekt hat eine INTEGER-Eigenschaft `source` mit der Knoten-Id am Anfang der Kante.
 * - Das `data`-Objekt hat eine INTEGER-Eigenschaft `target` mit der Knoten-Id am Ende der Kante.
 * - Das `data`-Objekt hat eine FLOAT-Eigenschaft `length_m` mit der Länge der Kante in Meter.
 * - Das `data`-Objekt hat eine GEOMETRY-Eigenschaft mit einer LINE_STRING-Geometrie.
 * - Das `data`-Objekt kann eine FLOAT-Eigenschaft `duration_forward_s` mit der Dauer für das Fahren der Kante in positiver Richtung haben.
 * - Das `data`-Objekt kann eine FLOAT-Eigenschaft `duration_backward_s` mit der Dauer für das Fahren der Kante in negativer Richtung haben.
 * - Das `data`-Objekt kann eine FLOAT-Eigenschaft `maxHeight_m` mit der maximalen Fahrzeughöhe in Meter für die Kante haben.
 * - Das `data`-Objekt kann eine FLOAT-Eigenschaft `maxWeight_m` mit dem maximalen Fahrzeuggewicht in Tonnen für die Kante haben.
 * - Das `data`-Objekt kann eine FLOAT-Eigenschaft `maxspeed_forward` mit der Geschwindigkeitsbegrenzung auf der Kante in positiver Richtung haben.
 * - Das `data`-Objekt kann eine FLOAT-Eigenschaft `maxspeed_backward` mit der Geschwindigkeitsbegrenzung am Rand in negativer Richtung haben.
 *     </code>
 * @ref:propertyTable {@link de.ii.xtraplatform.routes.sql.domain.ImmutableRoutesConfiguration}
 * @ref:example {@link de.ii.xtraplatform.routes.sql.domain.RoutesConfiguration}
 */
@Singleton
@AutoBind
public class RoutesQueriesSql implements FeatureQueriesExtension {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoutesQueriesSql.class);

  private final CrsTransformerFactory crsTransformerFactory;

  @Inject
  public RoutesQueriesSql(CrsTransformerFactory crsTransformerFactory) {
    this.crsTransformerFactory = crsTransformerFactory;
  }

  @Override
  public boolean isSupported(
      FeatureProviderConnector<?, ?, ?> connector, FeatureProviderDataV2 data) {
    return connector instanceof SqlConnector;
  }

  @Override
  public void on(
      LIFECYCLE_HOOK hook,
      FeatureProviderEntity provider,
      FeatureProviderConnector<?, ?, ?> connector) {
    Optional<RoutesConfiguration> routesConfiguration = getRoutesConfiguration(provider.getData());

    if (routesConfiguration.isPresent()) {
      SqlClient sqlClient = ((SqlConnector) connector).getSqlClient();

      switch (hook) {
        case STARTED:
          if (routesConfiguration.get().shouldWarmup()) {
            LOGGER.debug("Warming up routes queries for {}", provider.getId());
            List<String> queries = getWarmupSelects(routesConfiguration.get());
            AtomicInteger completed = new AtomicInteger();
            queries.forEach(
                query ->
                    sqlClient
                        .run(query, SqlQueryOptions.ignoreResults())
                        .whenComplete(
                            (r, t) -> {
                              completed.getAndIncrement();
                              if (completed.get() == queries.size())
                                LOGGER.debug(
                                    "Routes queries for {} are warmed up", provider.getId());
                            }));
          }
          break;
      }
    }
  }

  @Override
  public void on(
      QUERY_HOOK hook,
      FeatureProviderDataV2 data,
      FeatureProviderConnector<?, ?, ?> connector,
      Query query,
      BiConsumer<String, String> aliasResolver) {
    if (!(query instanceof FeatureQuery)) {
      return;
    }
    Optional<RoutesConfiguration> routesConfiguration = getRoutesConfiguration(data);
    Optional<RouteQuery> routeQuery = getRouteQuery((FeatureQuery) query);

    if (routesConfiguration.isPresent() && routeQuery.isPresent()) {
      SqlClient sqlClient = ((SqlConnector) connector).getSqlClient();

      switch (hook) {
        case BEFORE:
          String tableName =
              createRouteTable(routesConfiguration.get(), routeQuery.get(), sqlClient);
          aliasResolver.accept("_route_", tableName);
          break;
        case AFTER:
          deleteRouteTable(routesConfiguration.get(), routeQuery.get(), sqlClient);
          break;
      }
    }
  }

  private String createRouteTable(
      RoutesConfiguration routesConfiguration, RouteQuery routeQuery, SqlClient sqlClient) {
    String tableName = getTempTableName(routeQuery);

    createTempTable(sqlClient, tableName);

    getRouteSelects(routesConfiguration, routeQuery)
        .forEach(select -> insertIntoTempTable(sqlClient, tableName, select));

    return tableName;
  }

  private void deleteRouteTable(
      RoutesConfiguration routesConfiguration, RouteQuery routeQuery, SqlClient sqlClient) {
    String tableName = getTempTableName(routeQuery);

    deleteTempTable(sqlClient, tableName);
  }

  private void createTempTable(SqlClient sqlClient, String name) {
    String drop = String.format("DROP TABLE IF EXISTS %1$s;", name);

    sqlClient.run(drop, SqlQueryOptions.ddl()).join();

    String create =
        String.format(
            "CREATE TABLE %1$s (id serial, seq int, path_seq int, node bigint, edge bigint, cost float, agg_cost float)",
            name);

    sqlClient.run(create, SqlQueryOptions.ddl()).join();
  }

  private void insertIntoTempTable(SqlClient sqlClient, String name, String select) {
    String query =
        String.format(
            "INSERT INTO %s(seq, path_seq, node, edge, cost, agg_cost) (%s)", name, select);

    sqlClient
        .run(query, SqlQueryOptions.mutation())
        .whenComplete(
            (result, throwable) -> {
              if (Objects.nonNull(throwable)) {
                try {
                  LOGGER.debug(
                      "Inserting into temp table failed, dropping it ({})", throwable.getMessage());
                  deleteTempTable(sqlClient, name);
                } catch (Throwable t) {
                  // ignore
                }
              }
            })
        .join();
  }

  private void deleteTempTable(SqlClient sqlClient, String name) {
    String query = String.format("DROP TABLE IF EXISTS %s", name);

    sqlClient.run(query, SqlQueryOptions.ddl()).join();
  }

  private String getTempTableName(RouteQuery query) {
    return String.format("r_%d", query.hashCode()).replaceAll("-", "_");
  }

  private List<String> getRouteSelects(RoutesConfiguration cfg, RouteQuery query) {

    return getSegments(query, cfg.getNativeCrs()).stream()
        .map(
            segment ->
                getRouteSelect(
                    cfg,
                    segment.first(),
                    segment.second(),
                    query.getCostColumn(),
                    query.getReverseCostColumn(),
                    query.getMode(),
                    query.getFlags(),
                    query.getWeight(),
                    query.getHeight(),
                    query.getObstacles()))
        .collect(Collectors.toList());
  }

  private String getRouteSelect(
      RoutesConfiguration cfg,
      Point start,
      Point end,
      String costColumn,
      String reverseCostColumn,
      String mode,
      List<String> flags,
      Optional<Double> weight,
      Optional<Double> height,
      Optional<MultiPolygon> obstacles) {

    int mask = 0;
    for (String flag : flags) {
      if (cfg.getFlags().containsKey(flag)) {
        mask |= cfg.getFlags().get(flag);
      }
    }

    String fromToQuery =
        Objects.requireNonNull(
            cfg.getFromToQuery().get(mode),
            "Invalid Route Provider configuration. Mode '"
                + mode
                + "' is supported by the API configuration, but not the 'fromToQuery' in the provider.");
    String edgesQuery =
        Objects.requireNonNull(
            cfg.getEdgesQuery().get(mode),
            "Invalid Route Provider configuration. Mode '"
                + mode
                + "' is supported by the API configuration, but not the 'edgesQuery' in the provider.");

    String select =
        cfg.getRouteQuery()
            .replace("${edgesQuery}", "'" + edgesQuery.replaceAll("'", "''") + "'")
            .replace("${from_vid}", "(SELECT oid FROM start_pnt LIMIT 1)")
            .replace("${to_vid}", "(SELECT oid FROM end_pnt LIMIT 1)")
            .replace("${flag_mask}", String.valueOf(mask))
            .replace("${cost_column}", costColumn)
            .replace("${reverse_cost_column}", reverseCostColumn);

    if (weight.isPresent()) {
      select = select.replace("${weight}", String.valueOf(weight.get()));
    } else {
      select = select.replace("${weight}", cfg.getWeightDefault().replaceAll("'", "''"));
    }

    if (height.isPresent()) {
      select = select.replace("${height}", String.valueOf(height.get()));
    } else {
      select = select.replace("${height}", cfg.getHeightDefault().replaceAll("'", "''"));
    }

    if (obstacles.isPresent()) {
      select =
          select.replace(
              "${obstacles}",
              "ST_GeomFromText(''"
                  + getWkt(obstacles.get())
                  + "'',"
                  + obstacles.get().getCrs().orElse(OgcCrs.CRS84).getCode()
                  + ")");
    } else {
      select = select.replace("${obstacles}", cfg.getObstaclesDefault().replaceAll("'", "''"));
    }

    return "WITH\n"
        + " pnts AS (SELECT ST_GeomFromText('POINT("
        + start.getValue().x()
        + " "
        + start.getValue().y()
        + ")', "
        + cfg.getNativeCrs().getCode()
        + ") AS pnt1,\n"
        + "                 ST_GeomFromText('POINT("
        + end.getValue().x()
        + " "
        + end.getValue().y()
        + ")', "
        + cfg.getNativeCrs().getCode()
        + ") AS pnt2),\n"
        + " start_pnt AS ("
        + fromToQuery.replaceAll("\\$\\{point\\}", "(SELECT pnt1 FROM pnts)")
        + "),\n"
        + " end_pnt AS ("
        + fromToQuery.replaceAll("\\$\\{point\\}", "(SELECT pnt2 FROM pnts)")
        + ")\n"
        + select
        + " AS segment WHERE edge != -1";
  }

  private List<Tuple<Point, Point>> getSegments(RouteQuery query, EpsgCrs nativeCrs) {
    List<Tuple<Point, Point>> segments = new ArrayList<>();

    Point last = withCrs(query.getStart(), nativeCrs);

    for (Point wayPoint : query.getWayPoints()) {
      Point current = withCrs(wayPoint, nativeCrs);
      segments.add(Tuple.of(last, current));
      last = current;
    }

    segments.add(Tuple.of(last, withCrs(query.getEnd(), nativeCrs)));

    return segments;
  }

  private Point withCrs(Point point, EpsgCrs targetCrs) {
    EpsgCrs sourceCrs = point.getCrs().orElse(OgcCrs.CRS84);
    if (!Objects.equals(sourceCrs, targetCrs)) {
      Optional<CrsTransformer> transformer =
          crsTransformerFactory.getTransformer(sourceCrs, targetCrs);
      if (transformer.isPresent()) {
        CoordinateTuple coordinateTuple =
            transformer.get().transform(point.getValue().x(), point.getValue().y());
        return Point.of(coordinateTuple.getX(), coordinateTuple.getY(), targetCrs);
      }
    }
    return point;
  }

  private List<String> getWarmupSelects(RoutesConfiguration cfg) {
    return cfg.getEdgesQuery().values().stream()
        .map(
            edgesQuery ->
                cfg.getPreferences().values().stream()
                    .map(
                        preference ->
                            cfg.getRouteQuery()
                                .replace(
                                    "${edgesQuery}", "'" + edgesQuery.replaceAll("'", "''") + "'")
                                .replace("${from_vid}", "0")
                                .replace("${to_vid}", "0")
                                .replace("${flag_mask}", "0")
                                .replace("${cost_column}", preference.getCostColumn())
                                .replace(
                                    "${reverse_cost_column}", preference.getReverseCostColumn())
                                .replace("${height}", cfg.getHeightDefault())
                                .replace("${weight}", cfg.getWeightDefault())
                                .replace("${obstacles}", cfg.getObstaclesDefault())
                                .replace("${best ? 1 : -1}", "-1"))
                    .collect(Collectors.toUnmodifiableList()))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<RouteQuery> getRouteQuery(FeatureQuery query) {
    if (Objects.isNull(query)) {
      return Optional.empty();
    }
    return query.getExtensions().stream()
        .filter(extension -> extension instanceof RouteQuery)
        .map(extension -> (RouteQuery) extension)
        .findFirst();
  }

  private Optional<RoutesConfiguration> getRoutesConfiguration(FeatureProviderDataV2 data) {
    return data.getExtensions().stream()
        .filter(extension -> extension.isEnabled() && extension instanceof RoutesConfiguration)
        .map(extension -> (RoutesConfiguration) extension)
        .findFirst();
  }

  private String getWkt(MultiPolygon multiPolygon) {
    try {
      return new GeometryEncoderWkt().encode(multiPolygon);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
