/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import static de.ii.xtraplatform.tiles.app.TileGeneratorFeatures.EMPTY_TILES;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.AppConfiguration;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql.Format;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.FeatureQueriesExtension;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FilterEncoder;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.profile.ProfileSet;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.SqlConnector;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsPgis;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.tiles.domain.TileBuilder;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TilesetFeatures;
import de.ii.xtraplatform.web.domain.DropwizardPlugin;
import io.dropwizard.core.setup.Environment;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileBuilderPgisAsMvt
    implements FeatureQueriesExtension, TileBuilder, DropwizardPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileBuilderPgisAsMvt.class);

  private final Cql cql;
  private final Map<String, String> queryTemplates;
  private final Map<String, Timer> timers;

  private SqlConnector sqlConnector;
  private FilterEncoder<String> filterEncoder;
  private MetricRegistry metricRegistry;

  @Inject
  public TileBuilderPgisAsMvt(Cql cql) {
    this.cql = cql;
    this.queryTemplates = Collections.synchronizedMap(new HashMap<>());
    this.timers = new ConcurrentHashMap<>();
  }

  @Override
  public void init(AppConfiguration configuration, Environment environment) {
    this.metricRegistry = environment.metrics();
  }

  @Override
  public boolean isSupported(
      FeatureProviderConnector<?, ?, ?> connector, FeatureProviderDataV2 data) {
    if (connector instanceof SqlConnector
        && Objects.equals(((SqlConnector) connector).getDialect(), SqlDbmsPgis.ID)
        && (data instanceof FeatureProviderSqlData)
        && getConfiguration(data.getExtensions()).isPresent()) {
      // TODO: check mapping: ignore, warn, error
      return true;
    }

    return false;
  }

  @Override
  public void on(
      LIFECYCLE_HOOK hook,
      FeatureProviderEntity provider,
      FeatureProviderConnector<?, ?, ?> connector) {
    if (hook == LIFECYCLE_HOOK.STARTED
        && provider instanceof FilterEncoder
        && isSupported(connector, provider.getData())) {
      this.sqlConnector = (SqlConnector) connector;
      this.filterEncoder = (FilterEncoder<String>) provider;
      if (!timers.containsKey(provider.getId())) {
        timers.put(
            provider.getId(),
            metricRegistry.timer(String.format("tiles.%s.generated.mvt", provider.getId())));
      }

      LOGGER.info("Optimized PostGIS MVT tile generation enabled");
    }
  }

  @Override
  public void on(
      QUERY_HOOK hook,
      FeatureProviderDataV2 data,
      FeatureProviderConnector<?, ?, ?> connector,
      Query query,
      BiConsumer<String, String> aliasResolver) {}

  @Override
  public int getPriority() {
    return 100;
  }

  @Override
  public boolean isApplicable() {
    return Objects.nonNull(sqlConnector);
  }

  @Override
  public byte[] getMvtData(
      TileQuery tileQuery,
      TilesetFeatures tileset,
      Set<FeatureSchema> types,
      EpsgCrs nativeCrs,
      Optional<BoundingBox> clippedBounds,
      FeatureProvider featureProvider,
      PropertyTransformations baseTransformations,
      List<ProfileSet> profileSets) {
    try (Context timed = timers.get(featureProvider.getId()).time()) {
      if (clippedBounds.isEmpty()) {
        return EMPTY_TILES.get(tileQuery.getMediaType());
      }

      // TODO: duplicate
      String featureType = tileset.getFeatureType().orElse(tileset.getId());
      FeatureSchema schema = featureProvider.info().getSchema(featureType).orElse(null);

      if (Objects.isNull(schema)) {
        throw new IllegalArgumentException(
            String.format(
                "Unknown feature type '%s' in tileset '%s'", featureType, tileset.getId()));
      }

      String query =
          queryToSql(
              tileset,
              schema,
              clippedBounds.get(),
              nativeCrs,
              tileQuery.getTileMatrixSet(),
              tileQuery.getLevel());
      timed.stop();

      Collection<SqlRow> result =
          sqlConnector
              .getSqlClient()
              .run(query, SqlQueryOptions.withColumnTypes(byte[].class))
              .join();

      if (!result.isEmpty()) {
        List<Object> row = result.iterator().next().getValues();
        if (!row.isEmpty()) {
          return (byte[]) row.get(0);
        }
      }
    } catch (Throwable e) {
      LOGGER.error("ASMVT gen: {}", e.getMessage());
    }

    return new byte[0];
  }

  private String queryToSql(
      TilesetFeatures tileset,
      FeatureSchema schema,
      BoundingBox bbox,
      EpsgCrs nativeCrs,
      TileMatrixSetBase tms,
      int level) {
    String key = tileset.getId() + "_" + tms.getId() + "_" + level;
    String envelope = envelopeToSql(bbox);
    EpsgCrs targetCrs = tms.getCrs();

    if (queryTemplates.containsKey(key)) {
      return String.format(queryTemplates.get(key), envelope, targetCrs.getCode());
    }

    String table = schema.getSourcePath().get().substring(1);
    String geomColumn =
        schema.getProperties().stream()
            .filter(property -> property.isPrimaryGeometry())
            .findFirst()
            .flatMap(SchemaBase::getSourcePath)
            .orElseThrow();
    String attrColumns =
        schema.getProperties().stream()
            .filter(
                property ->
                    property.isValue()
                        && !property.isSpatial()
                        && !property.isConstant()
                        && property.getSourcePath().isPresent()
                        && !property.getSourcePath().get().contains("/"))
            .map(property -> property.getSourcePath().get() + " AS " + property.getName())
            .collect(Collectors.joining(","));

    String filter = filtersToSql(tileset, schema, tms.getId(), level);

    String bounds = "bounds AS (SELECT %1$s AS geom, %1$s::box2d AS b2d)";
    String mvtgeom =
        String.format(
            "mvtgeom AS (SELECT ST_AsMVTGeom(ST_Transform(A.%1$s, %%2$s), bounds.b2d) AS geom, %2$s FROM %3$s A, bounds WHERE (%5$s AND ST_Intersects(A.%1$s, ST_Transform(bounds.geom, %4$s))))",
            geomColumn, attrColumns, table, nativeCrs.getCode(), filter);

    String template =
        String.format("WITH\n%s,\n%s\nSELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom", bounds, mvtgeom);

    queryTemplates.put(key, template);

    return String.format(template, envelope, targetCrs.getCode());
  }

  // Densify the edges a little so the envelope can be safely converted to other coordinate systems.
  private String envelopeToSql(BoundingBox bounds) {
    double density_factor = 4.0;
    double segSize = (bounds.getXmax() - bounds.getXmin()) / density_factor;
    return String.format(
        "ST_Segmentize(ST_MakeEnvelope(%s, %s, %s, %s, %s),%s)",
        bounds.getXmin(),
        bounds.getYmin(),
        bounds.getXmax(),
        bounds.getYmax(),
        bounds.getEpsgCrs().getCode(),
        segSize);
  }

  private Optional<PgisTilesConfiguration> getConfiguration(
      List<ExtensionConfiguration> extensions) {
    return extensions.stream()
        .filter(extension -> extension.isEnabled() && extension instanceof PgisTilesConfiguration)
        .map(extension -> (PgisTilesConfiguration) extension)
        .findFirst();
  }

  private String filtersToSql(
      TilesetFeatures tileset, FeatureSchema schema, String tmsId, int level) {
    List<String> filters =
        tileset.getFilters().getOrDefault(tmsId, List.of()).stream()
            .filter(levelFilter -> levelFilter.matches(level))
            .map(filter -> cql.read(filter.getFilter(), Format.TEXT))
            .map(filter -> filterEncoder.encode(filter, schema.getName()))
            .toList();

    return filters.isEmpty()
        ? "TRUE"
        : filters.stream().collect(Collectors.joining(" AND ", "(", ")"));
  }
}
