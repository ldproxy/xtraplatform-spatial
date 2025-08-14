/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.base.domain.AppConfiguration;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql.Format;
import de.ii.xtraplatform.cql.domain.Geometry.Bbox;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.SIntersects;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureQueries;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureStream;
import de.ii.xtraplatform.features.domain.FeatureStream.ResultReduced;
import de.ii.xtraplatform.features.domain.FeatureTokenEncoder;
import de.ii.xtraplatform.features.domain.ImmutableFeatureQuery;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.streams.domain.Reactive.SinkReduced;
import de.ii.xtraplatform.tiles.domain.ImmutableTileGenerationContext;
import de.ii.xtraplatform.tiles.domain.LevelTransformation;
import de.ii.xtraplatform.tiles.domain.TileBuilder;
import de.ii.xtraplatform.tiles.domain.TileCoordinates;
import de.ii.xtraplatform.tiles.domain.TileGenerationContext;
import de.ii.xtraplatform.tiles.domain.TileGenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileGenerationParametersTransient;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TilesetFeatures;
import de.ii.xtraplatform.web.domain.DropwizardPlugin;
import io.dropwizard.core.setup.Environment;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.measure.Unit;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.kortforsyningen.proj.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileBuilderDefault implements TileBuilder, DropwizardPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileBuilderDefault.class);

  private static final Map<
          MediaType, Function<TileGenerationContext, ? extends FeatureTokenEncoder<?>>>
      ENCODERS = ImmutableMap.of(FeatureEncoderMVT.FORMAT, FeatureEncoderMVT::new);

  private final CrsInfo crsInfo;
  private final Cql cql;
  private final Map<String, Timer> timers;

  private MetricRegistry metricRegistry;

  @Inject
  public TileBuilderDefault(CrsInfo crsInfo, Cql cql) {
    this.crsInfo = crsInfo;
    this.cql = cql;
    this.timers = new ConcurrentHashMap<>();
  }

  @Override
  public void init(AppConfiguration configuration, Environment environment) {
    this.metricRegistry = environment.metrics();
  }

  @Override
  public int getPriority() {
    return 1000;
  }

  @Override
  public boolean isApplicable(String featureProviderId) {
    return true;
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
      PropertyTransformations baseTransformations) {
    if (!timers.containsKey(featureProvider.getId())) {
      timers.put(
          featureProvider.getId(),
          metricRegistry.timer(String.format("tiles.%s.generated.mvt", featureProvider.getId())));
    }

    try (Context timed = timers.get(featureProvider.getId()).time()) {
      FeatureQuery featureQuery =
          getFeatureQuery(
              tileQuery,
              tileset,
              types,
              nativeCrs,
              clippedBounds,
              tileQuery.getGenerationParametersTransient(),
              featureProvider.queries().get());

      FeatureStream tileSource = featureProvider.queries().get().getFeatureStream(featureQuery);

      TileGenerationContext tileGenerationContext =
          new ImmutableTileGenerationContext.Builder()
              .parameters(tileset)
              .coordinates(tileQuery)
              .tileset(tileQuery.getTileset())
              .build();

      FeatureTokenEncoder<?> encoder =
          ENCODERS.get(tileQuery.getMediaType()).apply(tileGenerationContext);

      String featureType = tileset.getFeatureType().orElse(tileset.getId());
      FeatureSchema schema = featureProvider.info().getSchema(featureType).orElse(null);

      if (Objects.isNull(schema)) {
        throw new IllegalArgumentException(
            String.format(
                "Unknown feature type '%s' in tileset '%s'", featureType, tileset.getId()));
      }

      PropertyTransformations propertyTransformations =
          tileQuery
              .getGenerationParameters()
              .flatMap(TileGenerationParameters::getPropertyTransformations)
              .map(pt -> pt.mergeInto(baseTransformations))
              .orElse(baseTransformations);

      ResultReduced<byte[]> resultReduced =
          generateTile(tileSource, encoder, Map.of(featureType, propertyTransformations));

      return resultReduced.reduced();
    }
  }

  private ResultReduced<byte[]> generateTile(
      FeatureStream featureStream,
      FeatureTokenEncoder<?> encoder,
      Map<String, PropertyTransformations> propertyTransformations) {

    SinkReduced<Object, byte[]> featureSink = encoder.to(Sink.reduceByteArray());

    try {
      ResultReduced<byte[]> result =
          featureStream.runWith(featureSink, propertyTransformations).toCompletableFuture().join();

      if (!result.isSuccess()) {
        result.getError().ifPresent(FeatureStream::processStreamError);
      }

      return result;

    } catch (CompletionException e) {
      if (e.getCause() instanceof WebApplicationException) {
        throw (WebApplicationException) e.getCause();
      }
      throw new IllegalStateException("Feature stream error.", e.getCause());
    }
  }

  private FeatureQuery getFeatureQuery(
      TileQuery tile,
      TilesetFeatures tileset,
      Set<FeatureSchema> featureTypes,
      EpsgCrs nativeCrs,
      Optional<BoundingBox> clippedBounds,
      Optional<TileGenerationParametersTransient> userParameters,
      FeatureQueries featureQueries) {
    String featureType = tileset.getFeatureType().orElse(tileset.getId());
    FeatureSchema featureSchema =
        featureTypes.stream()
            .filter(type -> Objects.equals(type.getName(), featureType))
            .findFirst()
            .orElse(null);
    // TODO: validate tileset during provider startup
    if (featureSchema == null) {
      throw new IllegalStateException(
          String.format(
              "Tileset '%s' references feature type '%s', which does not exist.",
              tileset.getId(), featureType));
    }
    FeatureSchema queryablesSchema =
        featureQueries.getQueryablesSchema(featureSchema, List.of("*"), List.of(), ".", true);

    ImmutableFeatureQuery.Builder queryBuilder =
        ImmutableFeatureQuery.builder()
            .type(featureType)
            .limit(tileset.getFeatureLimit())
            .offset(0)
            .crs(tile.getTileMatrixSet().getCrs())
            .maxAllowableOffset(getMaxAllowableOffset(tile, nativeCrs));

    if (tileset.getFilters().containsKey(tile.getTileMatrixSet().getId())) {
      tileset.getFilters().get(tile.getTileMatrixSet().getId()).stream()
          .filter(levelFilter -> levelFilter.matches(tile.getLevel()))
          // TODO: parse and validate filter, preferably in hydration or provider startup
          .forEach(filter -> queryBuilder.addFilters(cql.read(filter.getFilter(), Format.TEXT)));
    }

    queryablesSchema
        .getFilterGeometry()
        .map(SchemaBase::getFullPathAsString)
        .ifPresentOrElse(
            spatialProperty -> {
              clippedBounds.ifPresentOrElse(
                  bbox ->
                      queryBuilder.addFilters(
                          SIntersects.of(
                              Property.of(spatialProperty), SpatialLiteral.of(Bbox.of(bbox)))),
                  () -> queryBuilder.addFilters(BooleanValue2.of(false)));
            },
            // TODO: validate feature schema during provider startup
            () -> queryBuilder.addFilters(BooleanValue2.of(false)));

    if (userParameters.isPresent()) {
      userParameters.get().getLimit().ifPresent(queryBuilder::limit);
      queryBuilder.addAllFilters(userParameters.get().getFilters());
      if (!userParameters.get().getFields().isEmpty()) {
        queryBuilder.addAllFields(userParameters.get().getFields());
      }
    }

    if ((userParameters.isEmpty() || userParameters.get().getFields().isEmpty())
        && tileset.getTransformations().containsKey(tile.getTileMatrixSet().getId())) {
      List<String> fields =
          tileset.getTransformations().get(tile.getTileMatrixSet().getId()).stream()
              .filter(rule -> rule.matches(tile.getLevel()))
              .map(LevelTransformation::getProperties)
              .flatMap(Collection::stream)
              .collect(Collectors.toList());
      if (!fields.isEmpty()) {
        Stream.concat(
                fields.stream(),
                queryablesSchema.getPrimaryGeometry().map(SchemaBase::getFullPathAsString).stream())
            .distinct()
            .forEach(queryBuilder::addFields);
      }
    }

    return queryBuilder.build();
  }

  public double getMaxAllowableOffset(TileCoordinates tile, EpsgCrs nativeCrs) {
    double maxAllowableOffsetTileMatrixSet =
        tile.getTileMatrixSet()
            .getMaxAllowableOffset(tile.getLevel(), tile.getRow(), tile.getCol());
    Unit<?> tmsCrsUnit = crsInfo.getUnit(tile.getTileMatrixSet().getCrs());
    Unit<?> nativeCrsUnit = crsInfo.getUnit(nativeCrs);
    if (tmsCrsUnit.equals(nativeCrsUnit)) {
      return maxAllowableOffsetTileMatrixSet;
    } else if (tmsCrsUnit.equals(Units.DEGREE) && nativeCrsUnit.equals(Units.METRE)) {
      return maxAllowableOffsetTileMatrixSet * 111333.0;
    } else if (tmsCrsUnit.equals(Units.METRE) && nativeCrsUnit.equals(Units.DEGREE)) {
      return maxAllowableOffsetTileMatrixSet / 111333.0;
    }

    LOGGER.warn(
        "Tile generation: cannot convert between axis units '{}' and '{}'.",
        tmsCrsUnit.getName(),
        nativeCrsUnit.getName());
    return 0;
  }
}
