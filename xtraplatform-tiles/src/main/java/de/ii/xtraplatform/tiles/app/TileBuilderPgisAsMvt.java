/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import static de.ii.xtraplatform.tiles.app.TileGeneratorFeatures.EMPTY_TILES;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.FeatureQueriesExtension;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.profile.ProfileSet;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.SqlConnector;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsPgis;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.tiles.domain.LevelFilter;
import de.ii.xtraplatform.tiles.domain.TileBuilder;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TilesetFeatures;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileBuilderPgisAsMvt implements FeatureQueriesExtension, TileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileBuilderPgisAsMvt.class);

  private SqlConnector sqlConnector;

  @Inject
  public TileBuilderPgisAsMvt() {}

  @Override
  public boolean isSupported(
      FeatureProviderConnector<?, ?, ?> connector, FeatureProviderDataV2 data) {
    if (connector instanceof SqlConnector
        && Objects.equals(((SqlConnector) connector).getDialect(), SqlDbmsPgis.ID)
        && (data instanceof FeatureProviderSqlData)
        && getConfiguration(data.getExtensions()).isPresent()) {
      LOGGER.debug("ASMVT enabled: {}", true);
      return true;
    }

    return false;
  }

  @Override
  public void on(
      LIFECYCLE_HOOK hook,
      FeatureProviderEntity provider,
      FeatureProviderConnector<?, ?, ?> connector) {
    if (isSupported(connector, provider.getData())) {
      this.sqlConnector = (SqlConnector) connector;
      LOGGER.debug("ASMVT connected: {}", connector.getDatasetIdentifier());
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
    if (clippedBounds.isEmpty()) {
      // no features in this tile
      // LOGGER.debug("ASMVT EMPTY");
      return EMPTY_TILES.get(tileQuery.getMediaType());
    }

    try {
      // LOGGER.debug("ASMVT gen: {}", sqlConnector.getSqlClient().getDbInfo());

      // TODO: duplicate
      String featureType = tileset.getFeatureType().orElse(tileset.getId());
      FeatureSchema schema = featureProvider.info().getSchema(featureType).orElse(null);

      if (Objects.isNull(schema)) {
        throw new IllegalArgumentException(
            String.format(
                "Unknown feature type '%s' in tileset '%s'", featureType, tileset.getId()));
      }

      List<LevelFilter> filters =
          tileset
              .getFilters()
              .getOrDefault(tileQuery.getTileMatrixSet().getId(), List.of())
              .stream()
              .filter(levelFilter -> levelFilter.matches(tileQuery.getLevel()))
              .toList();

      String query =
          queryToSql(
              schema,
              clippedBounds.get(),
              nativeCrs,
              tileQuery.getTileMatrixSet().getCrs(),
              tileQuery.getLevel(),
              filters);

      // LOGGER.debug("ASMVT SQL: {}", query);

      Collection<SqlRow> result =
          sqlConnector
              .getSqlClient()
              .run(query, SqlQueryOptions.withColumnTypes(byte[].class))
              .join();
      /*result.forEach(
      row -> {
        row.getValues().forEach(val -> LOGGER.debug("ASMVT result: {}", ((byte[]) val).length));
      });*/
      if (!result.isEmpty() && !result.iterator().next().getValues().isEmpty()) {
        return (byte[]) result.iterator().next().getValues().get(0);
      }
    } catch (Throwable e) {
      LOGGER.error("ASMVT gen: {}", e.getMessage());
    }

    return new byte[0];
  }

  public String queryToSql(
      FeatureSchema schema,
      BoundingBox bbox,
      EpsgCrs nativeCrs,
      EpsgCrs targetCrs,
      int level,
      List<LevelFilter> filters) {
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
                property -> property.isValue() && !property.isSpatial() && !property.isConstant())
            .map(SchemaBase::getSourcePath)
            .map(path -> path.get())
            .collect(Collectors.joining(","));

    // LOGGER.debug("SCHEMA: {} {} {}", table, geomColumn, attrColumns);

    String envelope = envelopeToSql(bbox);

    String filter =
        filters.isEmpty()
            ? "TRUE"
            : filters.stream()
                .map(LevelFilter::getFilter)
                .map(f -> "t." + f)
                .collect(Collectors.joining(" AND ", "(", ")"));

    // LOGGER.debug("FILTER: {}", filter);

    // TODO: parse and validate filter, preferably in hydration or provider startup
    // .forEach(filter -> queryBuilder.addFilters(cql.read(filter.getFilter(), Format.TEXT)));

    String bounds = String.format("bounds AS (SELECT %1$s AS geom, %1$s::box2d AS b2d)", envelope);
    String mvtgeom =
        String.format(
            "mvtgeom AS (SELECT ST_AsMVTGeom(ST_Transform(t.%1$s, %2$s), bounds.b2d) AS geom, %3$s FROM %4$s t, bounds WHERE (%6$s AND ST_Intersects(t.%1$s, ST_Transform(bounds.geom, %5$s))))",
            geomColumn, targetCrs.getCode(), attrColumns, table, nativeCrs.getCode(), filter);

    return String.format("WITH\n%s,\n%s\nSELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom", bounds, mvtgeom);
  }

  // Densify the edges a little so the envelope can be safely converted to other coordinate systems.
  public String envelopeToSql(BoundingBox bounds) {
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
}
