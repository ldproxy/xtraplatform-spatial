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
import de.ii.xtraplatform.crs.domain.EpsgCrs.Force;
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
import de.ii.xtraplatform.features.domain.SchemaBase.Scope;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.profile.ProfileSet;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.SqlConnector;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsPgis;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.tiles.app.PgisTilesConfiguration.UnsupportedMode;
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

/**
 * @title PostGIS Tiles
 * @langEn Optimized vector tile generation using `ST_AsMVT` from PostGIS.
 * @langDe Optimierte Generierung von Vektor-Kacheln mit `ST_AsMVT` von PostGIS.
 * @scopeEn It is highly recommended to enable this extension for simple mappings (see limitations).
 *     That will speed up vector tile generation roughly by a factor of 4.
 * @scopeDe Es wird stark empfohlen, diese Erweiterung für einfache Mappings zu aktivieren (siehe
 *     Limitierungen). Dadurch wird die Generierung von Vektor-Kacheln ca. um den Faktor 4
 *     beschleunigt.
 * @limitationsEn Only simple value properties are supported. That means no objects, arrays,
 *     transformations or joins. Such properties may be excluded from the generated tiles, see
 *     option `unsupportedProperties`. The native CRS must not have a forced axis order.
 * @limitationsDe Es werden nur einfache Wert-Properties unterstützt. Das bedeutet keine Objekte,
 *     Arrays, Transformationen oder Joins. Solche Properties können von den generierten Kacheln
 *     ausgeschlossen werden, siehe die Option `unsupportedProperties`. Das native CRS darf keine
 *     erzwungene Achsenreihenfolge haben.
 * @ref:propertyTable {@link de.ii.xtraplatform.tiles.app.ImmutablePgisTilesConfiguration}
 * @ref:example {@link de.ii.xtraplatform.tiles.app.PgisTilesConfiguration}
 */
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
        && isSupported(connector, provider.getData())
        && isFeasible(provider.getData())) {
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
      BoundingBox tileBounds,
      Optional<BoundingBox> clippedBounds,
      FeatureProvider featureProvider,
      PropertyTransformations baseTransformations,
      List<ProfileSet> profileSets) {
    try (Context timed = timers.get(featureProvider.getId()).time()) {
      if (clippedBounds.isEmpty()) {
        return EMPTY_TILES.get(tileQuery.getMediaType());
      }

      if (!tileset.getProfiles().isEmpty() && LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Tileset '{}' has profiles, but profiles are not supported by the optimized PostGIS MVT tile generation. The profiles are ignored.",
            tileset.getId());
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
              tileBounds,
              clippedBounds,
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
      LOGGER.error(
          "Error during optimized PostGIS MVT tile generation, using an empty tile: {}",
          e.getMessage());
    }

    return new byte[0];
  }

  private String queryToSql(
      TilesetFeatures tileset,
      FeatureSchema schema,
      BoundingBox bbox,
      Optional<BoundingBox> effectiveBbox,
      EpsgCrs nativeCrs,
      TileMatrixSetBase tms,
      int level) {
    String key = tileset.getId() + "_" + tms.getId() + "_" + level;
    String tileEnvelope = envelopeToSql(bbox);
    String effectiveEnvelope = effectiveBbox.map(this::envelopeToSql).orElse(tileEnvelope);
    EpsgCrs targetCrs = tms.getCrs();

    if (queryTemplates.containsKey(key)) {
      return String.format(
          queryTemplates.get(key), effectiveEnvelope, tileEnvelope, targetCrs.getCode());
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
            .map(property -> property.getSourcePath().get() + " AS \"" + property.getName() + "\"")
            .collect(Collectors.joining(","));
    Optional<String> idProperty =
        schema.getProperties().stream()
            .filter(SchemaBase::isId)
            .filter(
                property ->
                    property.getSourcePath().isPresent() && Type.INTEGER.equals(property.getType()))
            .findFirst()
            .map(FeatureSchema::getName);

    String filter = filtersToSql(tileset, schema, tms.getId(), level);

    String bounds = "bounds AS (SELECT %1$s AS geom, %2$s::box2d AS b2d)";
    String mvtgeom =
        String.format(
            "mvtgeom AS (SELECT ST_AsMVTGeom(ST_Transform(A.%1$s, %%3$s), bounds.b2d, %6$d, 8) AS geom, %2$s FROM %3$s A, bounds WHERE (%5$s AND ST_Intersects(A.%1$s, ST_Transform(bounds.geom, %4$s))))",
            geomColumn, attrColumns, table, nativeCrs.getCode(), filter, tms.getTileExtent());

    String template =
        String.format(
            "WITH\n%s,\n%s\nSELECT ST_AsMVT(mvtgeom.*, '%s', %d, 'geom'%s) FROM mvtgeom",
            bounds,
            mvtgeom,
            schema.getName(),
            tms.getTileExtent(),
            idProperty.map(id -> ", '" + id + "'").orElse(""));

    queryTemplates.put(key, template);

    return String.format(template, effectiveEnvelope, tileEnvelope, targetCrs.getCode());
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

  private static Optional<PgisTilesConfiguration> getConfiguration(
      List<ExtensionConfiguration> extensions) {
    return extensions.stream()
        .filter(extension -> extension.isEnabled() && extension instanceof PgisTilesConfiguration)
        .map(extension -> (PgisTilesConfiguration) extension)
        .findFirst();
  }

  private static boolean isFeasible(FeatureProviderDataV2 data) {
    UnsupportedMode unsupportedMode =
        getConfiguration(data.getExtensions()).get().getUnsupportedProperties();
    List<String> objectOrArrayProperties =
        data.getTypes().values().stream()
            .flatMap(
                type ->
                    type.getProperties().stream()
                        .filter(
                            property ->
                                !property.getExcludedScopes().contains(Scope.RETURNABLE)
                                    && (property.isObject() || property.isArray()))
                        .map(property -> type.getName() + "." + property.getFullPathAsString()))
            .toList();
    List<String> transformedValueProperties =
        data.getTypes().values().stream()
            .flatMap(
                type ->
                    type.getProperties().stream()
                        .filter(
                            property ->
                                !property.getExcludedScopes().contains(Scope.RETURNABLE)
                                    && property.isValue()
                                    && !property.getTransformations().isEmpty())
                        .map(property -> type.getName() + "." + property.getFullPathAsString()))
            .toList();
    List<String> joinedValueProperties =
        data.getTypes().values().stream()
            .flatMap(
                type ->
                    type.getProperties().stream()
                        .filter(
                            property ->
                                !property.getExcludedScopes().contains(Scope.RETURNABLE)
                                    && property.isValue()
                                    && property.getSourcePath().isPresent()
                                    && property.getSourcePath().get().contains("/"))
                        .map(property -> type.getName() + "." + property.getFullPathAsString()))
            .toList();
    boolean hasCrsAxisSwap =
        data.getNativeCrs().isPresent()
            && data.getNativeCrs().get().getForceAxisOrder() == Force.LAT_LON;

    boolean isFeasible = true;
    String skipping = unsupportedMode == UnsupportedMode.WARN ? ", skipping" : "";
    String skippingTransformations =
        unsupportedMode == UnsupportedMode.WARN ? ", skipping transformations in properties" : "";

    if (!objectOrArrayProperties.isEmpty()) {
      if (unsupportedMode != UnsupportedMode.IGNORE) {
        LOGGER.warn(
            "Optimized PostGIS MVT tile generation is not supported for object and array properties{}: {}",
            skipping,
            String.join(", ", objectOrArrayProperties));
      }
      if (unsupportedMode == UnsupportedMode.DISABLE) {
        isFeasible = false;
      }
    }
    if (!transformedValueProperties.isEmpty()) {
      if (unsupportedMode != UnsupportedMode.IGNORE) {
        LOGGER.warn(
            "Optimized PostGIS MVT tile generation is not supported for value transformations{}: {}",
            skippingTransformations,
            String.join(", ", transformedValueProperties));
      }
      if (unsupportedMode == UnsupportedMode.DISABLE) {
        isFeasible = false;
      }
    }
    if (!joinedValueProperties.isEmpty()) {
      if (unsupportedMode != UnsupportedMode.IGNORE) {
        LOGGER.warn(
            "Optimized PostGIS MVT tile generation is not supported for joined value properties{}: {}",
            skipping,
            String.join(", ", joinedValueProperties));
      }
      if (unsupportedMode == UnsupportedMode.DISABLE) {
        isFeasible = false;
      }
    }
    if (hasCrsAxisSwap) {
      LOGGER.warn(
          "Optimized PostGIS MVT tile generation is not supported for native CRS with forced axis: {}",
          data.getNativeCrs().get());
      isFeasible = false;
    }

    if (!isFeasible) {
      LOGGER.warn("Optimized PostGIS MVT tile generation disabled");
      return false;
    }

    return true;
  }
}
