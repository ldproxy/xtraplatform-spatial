/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.resiliency.AbstractVolatileComposed;
import de.ii.xtraplatform.base.domain.resiliency.DelayedVolatile;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.base.domain.resiliency.VolatileUnavailableException;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql.Format;
import de.ii.xtraplatform.cql.domain.Geometry.Bbox;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.SIntersects;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.CrsTransformationException;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.FeatureQueries;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureStream;
import de.ii.xtraplatform.features.domain.FeatureStream.ResultReduced;
import de.ii.xtraplatform.features.domain.FeatureTokenEncoder;
import de.ii.xtraplatform.features.domain.ImmutableFeatureQuery;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.profile.ImmutableProfileTransformations;
import de.ii.xtraplatform.features.domain.profile.ProfileSet;
import de.ii.xtraplatform.features.domain.profile.ProfileSetRel;
import de.ii.xtraplatform.features.domain.profile.ProfileSetVal;
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.domain.transform.WithTransformationsApplied;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.streams.domain.Reactive.SinkReduced;
import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.ImmutableTileGenerationContext;
import de.ii.xtraplatform.tiles.domain.LevelTransformation;
import de.ii.xtraplatform.tiles.domain.TileCoordinates;
import de.ii.xtraplatform.tiles.domain.TileGenerationContext;
import de.ii.xtraplatform.tiles.domain.TileGenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileGenerationParametersTransient;
import de.ii.xtraplatform.tiles.domain.TileGenerationSchema;
import de.ii.xtraplatform.tiles.domain.TileGenerator;
import de.ii.xtraplatform.tiles.domain.TileProviderFeaturesData;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles.domain.TilesetFeatures;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.measure.Unit;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.kortforsyningen.proj.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileGeneratorFeatures extends AbstractVolatileComposed
    implements TileGenerator, ChainedTileProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileGeneratorFeatures.class);
  private static final Map<
          MediaType, Function<TileGenerationContext, ? extends FeatureTokenEncoder<?>>>
      ENCODERS = ImmutableMap.of(FeatureEncoderMVT.FORMAT, FeatureEncoderMVT::new);
  private static final Map<MediaType, byte[]> EMPTY_TILES =
      ImmutableMap.of(FeatureEncoderMVT.FORMAT, FeatureEncoderMVT.EMPTY_TILE);
  private static final Map<MediaType, PropertyTransformations> TRANSFORMATIONS =
      ImmutableMap.of(
          FeatureEncoderMVT.FORMAT,
          () ->
              ImmutableMap.of(
                  PropertyTransformations.WILDCARD,
                  ImmutableList.of(
                      new ImmutablePropertyTransformation.Builder().flatten(".").build())));
  private static final double BUFFER_DEGREE = 0.00001;
  private static final double BUFFER_METRE = 10.0;

  private final CrsInfo crsInfo;
  private final CrsTransformerFactory crsTransformerFactory;
  private final EntityRegistry entityRegistry;
  private final TileProviderFeaturesData data;
  private final Cql cql;
  private final Map<String, DelayedVolatile<FeatureProvider>> featureProviders;
  private final List<ProfileSet> profileSets;
  private final boolean async;

  public TileGeneratorFeatures(
      TileProviderFeaturesData data,
      CrsInfo crsInfo,
      CrsTransformerFactory crsTransformerFactory,
      EntityRegistry entityRegistry,
      Cql cql,
      VolatileRegistry volatileRegistry,
      boolean asyncStartup) {
    super("generator", volatileRegistry, true);
    this.data = data;
    this.crsInfo = crsInfo;
    this.crsTransformerFactory = crsTransformerFactory;
    this.entityRegistry = entityRegistry;
    this.cql = cql;
    this.featureProviders = new LinkedHashMap<>();
    this.profileSets = List.of(new ProfileSetRel(), new ProfileSetVal());
    this.async = asyncStartup;

    if (async) {
      init(volatileRegistry);
    } else {
      setState(State.AVAILABLE);
    }
  }

  private void init(VolatileRegistry volatileRegistry) {
    onVolatileStart();

    for (TilesetFeatures tileset : data.getTilesets().values()) {
      String featureProviderId =
          tileset
              .mergeDefaults(data.getTilesetDefaults())
              .getFeatureProvider()
              .orElse(TileProviderFeatures.clean(data.getId()));

      if (featureProviders.containsKey(featureProviderId)) {
        continue;
      }

      DelayedVolatile<FeatureProvider> delayedVolatile =
          new DelayedVolatile<>(
              volatileRegistry,
              String.format("generator.%s", featureProviderId),
              false,
              "generation");

      addSubcomponent(delayedVolatile);

      featureProviders.putIfAbsent(featureProviderId, delayedVolatile);

      entityRegistry
          .getEntity(FeatureProviderEntity.class, featureProviderId)
          .ifPresent(delayedVolatile::set);
    }

    onVolatileStarted();
  }

  private FeatureProvider getFeatureProvider(TilesetFeatures tileset) {
    String featureProviderId =
        tileset.getFeatureProvider().orElse(TileProviderFeatures.clean(data.getId()));

    if (async) {
      DelayedVolatile<FeatureProvider> provider = featureProviders.get(featureProviderId);

      // TODO: only crs, extents, queries needed
      if (!provider.isAvailable()) {
        throw new VolatileUnavailableException(
            String.format("Feature provider with id '%s' is not available.", featureProviderId));
      }

      return provider.get();
    }

    return entityRegistry
        .getEntity(FeatureProviderEntity.class, featureProviderId)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("Feature provider with id '%s' not found.", featureProviderId)));
  }

  Optional<FeatureProvider> getFeatureProvider(String featureProviderId) {
    if (async) {
      DelayedVolatile<FeatureProvider> provider = featureProviders.get(featureProviderId);

      if (!provider.isAvailable()) {
        return Optional.empty();
      }

      return Optional.of(provider.get());
    }

    return entityRegistry
        .getEntity(FeatureProviderEntity.class, featureProviderId)
        .map(fpe -> (FeatureProvider) fpe);
  }

  @Override
  public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
    return data.getTmsRanges();
  }

  @Override
  public boolean canProvide(TileQuery tile) {
    return ChainedTileProvider.super.canProvide(tile)
        && data.getTilesets().get(tile.getTileset()).getCombine().isEmpty();
  }

  @Override
  public TileResult getTile(TileQuery tile) {
    return TileResult.found(generateTile(tile));
  }

  @Override
  public boolean supports(MediaType mediaType) {
    return ENCODERS.containsKey(mediaType);
  }

  @Override
  public byte[] generateTile(TileQuery tileQuery) {
    if (!ENCODERS.containsKey(tileQuery.getMediaType())) {
      throw new IllegalArgumentException(
          String.format("Encoding not supported: %s", tileQuery.getMediaType()));
    }

    FeatureStream tileSource = getTileSource(tileQuery);
    if (tileSource == null) {
      // no features in this tile
      return EMPTY_TILES.get(tileQuery.getMediaType());
    }

    TilesetFeatures tileset =
        data.getTilesets().get(tileQuery.getTileset()).mergeDefaults(data.getTilesetDefaults());

    TileGenerationContext tileGenerationContext =
        new ImmutableTileGenerationContext.Builder()
            .parameters(tileset)
            .coordinates(tileQuery)
            .tileset(tileQuery.getTileset())
            .build();

    FeatureTokenEncoder<?> encoder =
        ENCODERS.get(tileQuery.getMediaType()).apply(tileGenerationContext);

    FeatureProvider featureProvider = getFeatureProvider(tileset);
    String featureType = tileset.getFeatureType().orElse(tileset.getId());
    FeatureSchema schema = featureProvider.info().getSchema(featureType).orElse(null);

    if (Objects.isNull(schema)) {
      throw new IllegalArgumentException(
          String.format("Unknown feature type '%s' in tileset '%s'", featureType, tileset.getId()));
    }

    final PropertyTransformations baseTransformations =
        getPropertyTransformations(tileset, schema, tileQuery.getMediaType());
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

  private PropertyTransformations getPropertyTransformations(
      TilesetFeatures tileset, FeatureSchema schema, MediaType mediaType) {
    ImmutableProfileTransformations.Builder transformationBuilder =
        new ImmutableProfileTransformations.Builder().from(TRANSFORMATIONS.get(mediaType));
    tileset
        .getProfiles()
        .forEach(
            profile -> {
              profileSets.stream()
                  .filter(p -> profile.startsWith(p.getPrefix()))
                  .forEach(
                      p ->
                          p.addPropertyTransformations(
                              profile, schema, mediaType.toString(), transformationBuilder));
            });
    return transformationBuilder.build();
  }

  @Override
  public FeatureStream getTileSource(TileQuery tileQuery) {
    TilesetFeatures tileset =
        data.getTilesets().get(tileQuery.getTileset()).mergeDefaults(data.getTilesetDefaults());
    FeatureProvider featureProvider = getFeatureProvider(tileset);

    if (!featureProvider.queries().isSupported()) {
      throw new IllegalStateException("Feature provider has no Queries support.");
    }
    if (!featureProvider.crs().isSupported()) {
      throw new IllegalStateException("Feature provider has no CRS support.");
    }

    // if the tileset is sparse, check, if the tile is outside the extent of the feature data;
    // if yes, return null
    if (Boolean.TRUE.equals(tileset.getSparse()) && featureProvider.extents().isAvailable()) {
      String featureType = tileset.getFeatureType().orElse(tileQuery.getTileset());
      if (featureProvider
          .extents()
          .get()
          .getSpatialExtent(featureType)
          .filter(
              bboxFeatures -> {
                try {
                  return BoundingBox.intersects(
                      bboxFeatures,
                      tileQuery.getBoundingBox(bboxFeatures.getEpsgCrs(), crsTransformerFactory));
                } catch (CrsTransformationException e) {
                  // ignore, assume there are features
                  return true;
                }
              })
          // if the extent is empty, there are no features, so we can return null, too
          .isEmpty()) {
        return null;
      }
    }

    EpsgCrs nativeCrs = featureProvider.crs().get().getNativeCrs();
    Set<FeatureSchema> types = featureProvider.info().getSchemas();
    FeatureQuery featureQuery =
        getFeatureQuery(
            tileQuery,
            tileset,
            types,
            nativeCrs,
            getBounds(tileQuery),
            tileQuery.getGenerationParametersTransient(),
            featureProvider.queries().get());

    return featureProvider.queries().get().getFeatureStream(featureQuery);
  }

  private Optional<BoundingBox> getBounds(TileQuery tileQuery) {
    return tileQuery
        .getGenerationParameters()
        .flatMap(TileGenerationParameters::getClipBoundingBox)
        .flatMap(
            clipBoundingBox ->
                Objects.equals(
                        tileQuery.getBoundingBox().getEpsgCrs(), clipBoundingBox.getEpsgCrs())
                    ? Optional.of(clipBoundingBox)
                    : crsTransformerFactory
                        .getTransformer(
                            clipBoundingBox.getEpsgCrs(), tileQuery.getBoundingBox().getEpsgCrs())
                        .map(
                            transformer -> {
                              try {
                                return transformer.transformBoundingBox(clipBoundingBox);
                              } catch (CrsTransformationException e) {
                                // ignore
                                return clipBoundingBox;
                              }
                            }));
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

  // TODO: create on startup for all tilesets
  @Override
  public TileGenerationSchema getGenerationSchema(String tilesetId) {
    TilesetFeatures tileset =
        data.getTilesets().get(tilesetId).mergeDefaults(data.getTilesetDefaults());
    FeatureProvider featureProvider = getFeatureProvider(tileset);
    String featureType = tileset.getFeatureType().orElse(tilesetId);
    FeatureSchema featureSchema = featureProvider.info().getSchema(featureType).orElse(null);

    return new TileGenerationSchema() {
      @Override
      public SimpleFeatureGeometry getGeometryType() {
        return featureSchema.getEffectiveGeometryType();
      }

      @Override
      public Optional<String> getTemporalProperty() {
        return featureSchema
            .getPrimaryInterval()
            .map(
                interval ->
                    String.format(
                        "%s%s%s",
                        interval.first().getFullPathAsString(),
                        "/", // TODO use constant
                        interval.second().getFullPathAsString()))
            .or(() -> featureSchema.getPrimaryInstant().map(SchemaBase::getFullPathAsString));
      }

      @Override
      public Map<String, FeatureSchema> getProperties() {
        return featureSchema.getPropertyMap();
      }
    };
  }

  public FeatureSchema getVectorSchema(String tilesetId, MediaType encoding) {
    TilesetFeatures tileset = data.getTilesets().get(tilesetId);

    if (Objects.isNull(tileset)) {
      throw new IllegalArgumentException(String.format("Unknown tileset '%s'", tilesetId));
    }

    FeatureProvider featureProvider =
        getFeatureProvider(tileset.mergeDefaults(data.getTilesetDefaults()));
    String featureType = tileset.getFeatureType().orElse(tileset.getId());
    FeatureSchema featureSchema = featureProvider.info().getSchema(featureType).orElse(null);

    if (Objects.isNull(featureSchema)) {
      throw new IllegalArgumentException(
          String.format("Unknown feature type '%s' in tileset '%s'", featureType, tilesetId));
    }

    return featureSchema.accept(
        new WithTransformationsApplied(
            getPropertyTransformations(tileset, featureSchema, encoding)));
  }

  @Override
  public Optional<BoundingBox> getBounds(String tilesetId) {
    TilesetFeatures tileset = data.getTilesets().get(tilesetId);

    if (Objects.isNull(tileset)) {
      throw new IllegalArgumentException(String.format("Unknown tileset '%s'", tilesetId));
    }

    FeatureProvider featureProvider =
        getFeatureProvider(tileset.mergeDefaults(data.getTilesetDefaults()));

    if (!featureProvider.extents().isAvailable()) {
      return Optional.empty();
    }

    String featureType = tileset.getFeatureType().orElse(tileset.getId());

    return featureProvider.extents().get().getSpatialExtent(featureType, OgcCrs.CRS84);
  }

  private FeatureQuery getFeatureQuery(
      TileQuery tile,
      TilesetFeatures tileset,
      Set<FeatureSchema> featureTypes,
      EpsgCrs nativeCrs,
      Optional<BoundingBox> bounds,
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
            .limit(
                Optional.ofNullable(tileset.getFeatureLimit())
                    .orElse(data.getTilesetDefaults().getFeatureLimit()))
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
        .getPrimaryGeometry()
        .map(SchemaBase::getFullPathAsString)
        .ifPresentOrElse(
            spatialProperty -> {
              clip(tile.getBoundingBox(), bounds)
                  .ifPresentOrElse(
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

  /**
   * Reduce bbox to the area in which there is data to avoid coordinate transformation issues with
   * large scale and data that is stored in a regional, projected CRS. A small buffer is used to
   * avoid issues with point features and queries in other CRSs where features on the boundary of
   * the spatial extent are suddenly no longer included in the result.
   */
  private Optional<BoundingBox> clip(BoundingBox bbox, Optional<BoundingBox> limits) {
    if (limits.isEmpty()) {
      return Optional.of(bbox);
    }

    return BoundingBox.intersect2d(bbox, limits.get(), getBuffer(bbox.getEpsgCrs()));
  }

  private double getBuffer(EpsgCrs crs) {
    List<Unit<?>> units = crsInfo.getAxisUnits(crs);
    if (!units.isEmpty()) {
      return Units.METRE.equals(units.get(0)) ? BUFFER_METRE : BUFFER_DEGREE;
    }
    // fallback to meters
    return BUFFER_METRE;
  }
}
