/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.resiliency.AbstractVolatileComposed;
import de.ii.xtraplatform.base.domain.resiliency.DelayedVolatile;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.base.domain.resiliency.VolatileUnavailableException;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.Bbox;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql.Format;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.SIntersects;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsTransformationException;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.features.domain.CollectionMetadata;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureStream;
import de.ii.xtraplatform.features.domain.ImmutableFeatureQuery;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.tiles.domain.ImmutableTileMatrix;
import de.ii.xtraplatform.tiles.domain.ImmutableTileMatrixSetData;
import de.ii.xtraplatform.tiles.domain.ImmutableTileMatrixSetData.Builder;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesBoundingBox;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dBuilder;
import de.ii.xtraplatform.tiles3d.domain.Tile3dCoordinates;
import de.ii.xtraplatform.tiles3d.domain.Tile3dGenerationParameters;
import de.ii.xtraplatform.tiles3d.domain.Tile3dGenerator;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFeaturesData;
import de.ii.xtraplatform.tiles3d.domain.TileTree;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFeatures;
import de.ii.xtraplatform.tiles3d.domain.spec.Availability;
import de.ii.xtraplatform.tiles3d.domain.spec.BufferView;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableAvailability;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableBuffer;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableBufferView;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableSubtree;
import de.ii.xtraplatform.tiles3d.domain.spec.Subtree;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.locationtech.jts.shape.fractal.MortonCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tile3dGeneratorFeatures extends AbstractVolatileComposed implements Tile3dGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dGeneratorFeatures.class);
  private static ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new Jdk8Module())
          .registerModule(new GuavaModule())
          .configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, true)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          // for debugging
          // .enable(SerializationFeature.INDENT_OUTPUT)
          .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

  private final Cql cql;
  private final CrsTransformerFactory crsTransformerFactory;
  private final EntityRegistry entityRegistry;
  private final Set<Tile3dBuilder> tileBuilders;
  private final Tile3dProviderFeaturesData data;
  private final Map<String, Tileset3dFeatures> tilesets;
  private final Map<String, DelayedVolatile<FeatureProvider>> featureProviders;
  private final Map<String, BoundingBox> extents;
  private final boolean async;
  private final String label;
  private Optional<Tile3dBuilder> tileBuilder;

  public Tile3dGeneratorFeatures(
      Tile3dProviderFeaturesData data,
      Cql cql,
      CrsTransformerFactory crsTransformerFactory,
      EntityRegistry entityRegistry,
      VolatileRegistry volatileRegistry,
      Set<Tile3dBuilder> tileBuilders,
      AppContext appContext,
      boolean asyncStartup) {
    super("generator", volatileRegistry, true);
    this.data = data;
    this.cql = cql;
    this.crsTransformerFactory = crsTransformerFactory;
    this.entityRegistry = entityRegistry;
    this.tileBuilders = tileBuilders;
    this.tilesets = new LinkedHashMap<>();
    this.featureProviders = new LinkedHashMap<>();
    this.extents = new ConcurrentHashMap<>();
    this.async = asyncStartup;
    this.label = String.format("%s v%s", appContext.getName(), appContext.getVersion());
    this.tileBuilder = Optional.empty();
  }

  @Override
  public void init() {
    if (async) {
      initAsync(volatileRegistry);
    } else {
      setState(State.AVAILABLE);
      init2();
    }
  }

  @Override
  public String getLabel() {
    return label;
  }

  private void initAsync(VolatileRegistry volatileRegistry) {
    onVolatileStart();

    for (Tileset3dFeatures tileset : data.getTilesets().values()) {
      Tileset3dFeatures tileset3dFeatures = tileset.mergeDefaults(data.getTilesetDefaults());

      String featureProviderId =
          tileset3dFeatures.getFeatureProvider().orElse(Tile3dProvider.clean(data.getId()));

      if (featureProviders.containsKey(featureProviderId)) {
        continue;
      }

      DelayedVolatile<FeatureProvider> delayedVolatile =
          new DelayedVolatile<>(
              volatileRegistry,
              String.format("generator.%s", featureProviderId),
              false,
              "generation");

      addSubcomponent(delayedVolatile, true);

      featureProviders.putIfAbsent(featureProviderId, delayedVolatile);

      entityRegistry.addEntityListener(
          FeatureProviderEntity.class,
          fp -> {
            if (Objects.equals(fp.getId(), featureProviderId)) {
              delayedVolatile.set(fp);
            }
          },
          true);
    }

    onVolatileStarted();
  }

  @Override
  protected Tuple<State, String> volatileInit() {
    if (async) {
      init2();
    }
    return super.volatileInit();
  }

  private void init2() {
    for (Tileset3dFeatures tileset : data.getTilesets().values()) {
      Tileset3dFeatures tileset3dFeatures = tileset.mergeDefaults(data.getTilesetDefaults());
      tilesets.put(tileset3dFeatures.getId(), tileset3dFeatures);

      getBounds(tileset3dFeatures).ifPresent(bbox -> extents.put(tileset3dFeatures.getId(), bbox));
    }

    this.tileBuilder =
        tileBuilders.stream().min(Comparator.comparingInt(Tile3dBuilder::getPriority));
  }

  @Override
  public Optional<BoundingBox> getBounds(String tilesetId) {
    Tileset3dFeatures tileset = data.getTilesets().get(tilesetId);

    if (Objects.isNull(tileset)) {
      throw new IllegalArgumentException(String.format("Unknown tileset '%s'", tilesetId));
    }

    return getBounds(tileset);
  }

  private Optional<BoundingBox> getBounds(Tileset3dFeatures tileset) {
    if (extents.containsKey(tileset.getId())) {
      return Optional.of(extents.get(tileset.getId()));
    }

    FeatureProvider featureProvider = getFeatureProvider(tileset);

    if (!featureProvider.extents().isAvailable()) {
      return Optional.empty();
    }

    String featureType = tileset.getFeatureType().orElse(tileset.getId());
    Optional<BoundingBox> extent =
        featureProvider.extents().get().getSpatialExtent(featureType, OgcCrs.CRS84h);

    extent.ifPresent(bbox -> extents.put(tileset.getId(), bbox));

    return extent;
  }

  @Override
  public TileMatrixSetData getTileMatrixSetData(
      String tilesetId, String tmsId, Range<Integer> levels) {
    BoundingBox boundingBox = getBounds(tilesetId).orElseThrow();
    ImmutableTileMatrixSetData.Builder builder =
        new Builder()
            .id(tmsId)
            .crs(OgcCrs.CRS84h.toUriString())
            .orderedAxes(List.of("lon", "lat"))
            .boundingBox(
                new ImmutableTilesBoundingBox.Builder()
                    .lowerLeft(
                        BigDecimal.valueOf(boundingBox.getXmin()).setScale(7, RoundingMode.HALF_UP),
                        BigDecimal.valueOf(boundingBox.getYmin()).setScale(7, RoundingMode.HALF_UP))
                    .upperRight(
                        BigDecimal.valueOf(boundingBox.getXmax()).setScale(7, RoundingMode.HALF_UP),
                        BigDecimal.valueOf(boundingBox.getYmax()).setScale(7, RoundingMode.HALF_UP))
                    .crs(OgcCrs.CRS84.toUriString())
                    .build());

    for (int level = 0; level <= levels.upperEndpoint(); level++) {
      double resolution = 360.0 / (256 * Math.pow(2, level));
      builder.addTileMatrices(
          new ImmutableTileMatrix.Builder()
              .id(String.valueOf(level))
              .tileWidth(256)
              .tileHeight(256)
              .matrixWidth((long) Math.pow(2, level))
              .matrixHeight((long) Math.pow(2, level))
              // not needed for TileWalker, but mandatory
              .scaleDenominator(BigDecimal.valueOf(0))
              .cellSize(BigDecimal.valueOf(0))
              .pointOfOrigin(new BigDecimal[] {BigDecimal.valueOf(0), BigDecimal.valueOf(0)})
              .build());
    }

    return builder.build();
  }

  @Override
  public Subtree generateSubtree(String tilesetId, TileTree subtree) {
    Tileset3dFeatures tileset =
        data.getTilesets().get(tilesetId).mergeDefaults(data.getTilesetDefaults());
    FeatureProvider featureProvider = getFeatureProvider(tileset);
    BoundingBox fullBbox = getBounds(tileset).orElseThrow();

    int size = 0;
    for (int i = 0; i < tileset.getSubtreeLevels(); i++) {
      size += MortonCode.size(i);
    }
    byte[] tileAvailability = new byte[(size - 1) / 8 + 1];
    Arrays.fill(tileAvailability, (byte) 0);
    byte[] contentAvailability = new byte[(size - 1) / 8 + 1];
    Arrays.fill(contentAvailability, (byte) 0);
    byte[] childSubtreeAvailability =
        new byte[(MortonCode.size(tileset.getSubtreeLevels()) - 1) / 8 + 1];
    Arrays.fill(childSubtreeAvailability, (byte) 0);

    processZ(
        featureProvider,
        tileset,
        fullBbox,
        subtree.getLevel(),
        subtree.getCol(),
        subtree.getRow(),
        subtree.getLevel(),
        subtree.getCol(),
        subtree.getRow(),
        tileAvailability,
        contentAvailability,
        childSubtreeAvailability);

    return buildSubtree(
        tileset.getSubtreeLevels(),
        size,
        tileAvailability,
        contentAvailability,
        childSubtreeAvailability);
  }

  @Override
  public boolean isTileAvailable(Subtree parent, Tile3dCoordinates tile, int subtreeLevels) {
    final Availability contentAvailability = parent.getContentAvailability().get(0);

    final byte[] buffer =
        contentAvailability.getBitstream().isPresent()
            ? parent.getContentAvailabilityBin()
            : Subtree.EMPTY;
    boolean always =
        buffer.length == 0 && contentAvailability.getConstant().filter(c -> c == 1).isPresent();
    int localLevel = tile.getLevel() % subtreeLevels;

    return always
        || (buffer.length > 0
            && getAvailability(
                buffer,
                localLevel,
                TileTree.getMortonCurveIndex(localLevel, tile.getCol(), tile.getRow())));
  }

  @Override
  public boolean isSubtreeAvailable(Subtree parent, TileTree child, int subtreeLevels) {
    final Availability childSubtreeAvailability = parent.getChildSubtreeAvailability();
    final byte[] buffer =
        childSubtreeAvailability.getBitstream().isPresent()
            ? parent.getChildSubtreeAvailabilityBin()
            : Subtree.EMPTY;
    boolean always =
        buffer.length == 0
            && childSubtreeAvailability.getConstant().filter(c -> c == 1).isPresent();

    return always
        || (buffer.length > 0 && getAvailability(buffer, child.getMortonCurveIndex(subtreeLevels)));
  }

  @Override
  public byte[] generateTile(
      Tileset3dFeatures tileset, Tile3dCoordinates tile, Tile3dGenerationParameters parameters) {
    FeatureProvider featureProvider = getFeatureProvider(tileset);
    Tile3dBuilder builder =
        tileBuilder.orElseThrow(
            () -> new IllegalStateException("No applicable tile builder found"));

    BoundingBox tilesetBoundingBox = getBounds(tileset.getId()).orElseThrow();
    BoundingBox tileBoundingBox =
        computeTileBbox(tilesetBoundingBox, tile.getLevel(), tile.getCol(), tile.getRow());
    BoundingBox clipBoundingBox =
        getClipBoundingBox(parameters, tileBoundingBox.getEpsgCrs()).orElse(tilesetBoundingBox);
    Optional<Polygon> exclusionPolygon = computeExclusionPolygon(tile, clipBoundingBox);

    return builder.generateTile(
        tile,
        tileset,
        tileBoundingBox,
        exclusionPolygon,
        featureProvider,
        parameters.getApiId(),
        parameters.getCollectionId());
  }

  @Override
  public byte[] subtreeToBinary(Subtree subtree) {
    byte[] json;
    try {
      json = MAPPER.writeValueAsBytes(subtree);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          String.format("Could not write 3D Tiles subtree. Reason: %s", e.getMessage()), e);
    }

    int bufferLength =
        subtree.getTileAvailabilityBin().length
            + subtree.getContentAvailabilityBin().length
            + subtree.getChildSubtreeAvailabilityBin().length;
    int jsonPadding = (8 - json.length % 8) % 8;
    int bufferPadding = (8 - bufferLength % 8) % 8;

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(Subtree.MAGIC_SUBT);
      outputStream.write(Subtree.VERSION_1);

      outputStream.write(intToLittleEndianLong(json.length + jsonPadding));
      outputStream.write(intToLittleEndianLong(bufferLength + bufferPadding));

      outputStream.write(json);
      for (int i = 0; i < jsonPadding; i++) {
        outputStream.write(Subtree.JSON_PADDING);
      }

      if (subtree.getTileAvailabilityBin().length > 0) {
        outputStream.write(subtree.getTileAvailabilityBin());
      }
      if (subtree.getContentAvailabilityBin().length > 0) {
        outputStream.write(subtree.getContentAvailabilityBin());
      }
      if (subtree.getChildSubtreeAvailabilityBin().length > 0) {
        outputStream.write(subtree.getChildSubtreeAvailabilityBin());
      }
      for (int i = 0; i < bufferPadding; i++) {
        outputStream.write(Subtree.BIN_PADDING);
      }

      outputStream.flush();
    } catch (Exception e) {
      throw new IllegalStateException("Could not write 3D Tiles Subtree output", e);
    }

    return outputStream.toByteArray();
  }

  @Override
  public Subtree subtreeFromBinary(byte[] subtreeBytes) {
    if (!Arrays.equals(Arrays.copyOfRange(subtreeBytes, 0, 4), Subtree.MAGIC_SUBT)) {
      throw new IllegalStateException(
          String.format(
              "Invalid 3D Tiles subtree, invalid magic number. Found: %s",
              Arrays.toString(Arrays.copyOfRange(subtreeBytes, 0, 4))));
    }

    final int version = littleEndianIntToInt(subtreeBytes, 4);
    if (version != 1) {
      throw new IllegalStateException(
          String.format(
              "Unsupported 3D Tiles subtree, only version 1 is supported. Found: %d", version));
    }

    final int jsonLength = littleEndianLongToInt(subtreeBytes, 8);
    final byte[] jsonContent = Arrays.copyOfRange(subtreeBytes, 24, 24 + jsonLength);

    try {
      Subtree subtreeWithEmptyBuffers = MAPPER.readValue(jsonContent, Subtree.class);

      int jsonPadding = (8 - jsonLength % 8) % 8;
      int bufferOffset = 24 + jsonLength + jsonPadding;

      ImmutableSubtree.Builder builder = ImmutableSubtree.builder().from(subtreeWithEmptyBuffers);

      subtreeWithEmptyBuffers
          .getTileAvailability()
          .getBitstream()
          .ifPresent(
              i ->
                  builder.tileAvailabilityBin(
                      getBufferViewContent(
                          subtreeBytes, subtreeWithEmptyBuffers, bufferOffset, i)));

      subtreeWithEmptyBuffers
          .getContentAvailability()
          .get(0)
          .getBitstream()
          .ifPresent(
              i ->
                  builder.contentAvailabilityBin(
                      getBufferViewContent(
                          subtreeBytes, subtreeWithEmptyBuffers, bufferOffset, i)));

      subtreeWithEmptyBuffers
          .getChildSubtreeAvailability()
          .getBitstream()
          .ifPresent(
              i ->
                  builder.childSubtreeAvailabilityBin(
                      getBufferViewContent(
                          subtreeBytes, subtreeWithEmptyBuffers, bufferOffset, i)));

      return builder.build();
    } catch (Exception e) {
      throw new IllegalStateException("Could not read 3D Tiles Subtree output", e);
    }
  }

  private FeatureProvider getFeatureProvider(Tileset3dFeatures tileset) {
    String featureProviderId =
        tileset.getFeatureProvider().orElse(Tile3dProvider.clean(data.getId()));

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

  private void processZ(
      FeatureProvider featureProvider,
      Tileset3dFeatures tileset,
      BoundingBox fullBbox,
      int baseLevel,
      int xBase,
      int yBase,
      int level,
      int x0,
      int y0,
      byte[] tileAvailability,
      byte[] contentAvailability,
      byte[] childSubtreeAvailability) {

    if (level > tileset.getContentLevels().getMax()
        || level - baseLevel > tileset.getSubtreeLevels()) {
      return;
    }
    Optional<Cql2Expression> additionalFilter;
    int i0 = MortonCode.encode(x0 - xBase, y0 - yBase);

    for (int i = 0; i < ((level - baseLevel) == 0 ? 1 : 4); i++) {
      int x1 = x0 + (i % 2);
      int y1 = y0 + (i / 2);

      BoundingBox bbox = computeTileBbox(fullBbox, level, x1, y1);
      int relativeLevel = level - tileset.getContentLevels().getMin();
      additionalFilter =
          relativeLevel >= 0 && tileset.getTileFiltersOrDefault().size() > relativeLevel
              ? Optional.ofNullable(tileset.getTileFiltersOrDefault().get(relativeLevel))
                  .map(filter -> cql.read(filter, Format.TEXT))
              : Optional.empty();
      boolean hasData = hasData(featureProvider, tileset, bbox, additionalFilter);
      if (hasData) {
        if (level - baseLevel < tileset.getSubtreeLevels()) {
          setAvailability(tileAvailability, 0, level - baseLevel, i0 + i);
          if (relativeLevel >= 0) {
            if (tileset.getContentFilters().isEmpty()) {
              setAvailability(contentAvailability, 0, level - baseLevel, i0 + i);
            } else {
              additionalFilter =
                  tileset.getContentFilters().size() > relativeLevel
                      ? Optional.ofNullable(tileset.getContentFilters().get(relativeLevel))
                          .map(filter -> cql.read(filter, Format.TEXT))
                      : Optional.empty();
              hasData = hasData(featureProvider, tileset, bbox, additionalFilter);
              if (hasData) {
                setAvailability(contentAvailability, 0, level - baseLevel, i0 + i);
              }
            }
          }

          processZ(
              featureProvider,
              tileset,
              fullBbox,
              baseLevel,
              xBase * 2,
              yBase * 2,
              level + 1,
              x1 * 2,
              y1 * 2,
              tileAvailability,
              contentAvailability,
              childSubtreeAvailability);
        } else {
          setAvailability(
              childSubtreeAvailability,
              tileset.getSubtreeLevels(),
              tileset.getSubtreeLevels(),
              i0 + i);
        }
      }
    }
  }

  private Optional<BoundingBox> getClipBoundingBox(
      Tile3dGenerationParameters tileGenerationParameters, EpsgCrs targetCrs) {
    return tileGenerationParameters
        .getClipBoundingBox()
        .flatMap(
            clipBoundingBox ->
                Objects.equals(targetCrs, clipBoundingBox.getEpsgCrs())
                    ? Optional.of(clipBoundingBox)
                    : crsTransformerFactory
                        .getTransformer(clipBoundingBox.getEpsgCrs(), targetCrs)
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

  private static BoundingBox computeTileBbox(BoundingBox fullBbox, int level, int x, int y) {
    double dx = fullBbox.getXmax() - fullBbox.getXmin();
    double dy = fullBbox.getYmax() - fullBbox.getYmin();
    double factor = Math.pow(2, level);
    double xmin = fullBbox.getXmin() + dx / factor * x;
    double xmax = xmin + dx / factor;
    double ymin = fullBbox.getYmin() + dy / factor * y;
    double ymax = ymin + dy / factor;
    return BoundingBox.of(
        xmin,
        ymin,
        Objects.requireNonNull(fullBbox.getZmin()),
        xmax,
        ymax,
        Objects.requireNonNull(fullBbox.getZmax()),
        OgcCrs.CRS84h);
  }

  private static boolean hasData(
      FeatureProvider featureProvider,
      Tileset3dFeatures tileset,
      BoundingBox bbox,
      Optional<Cql2Expression> additionalFilter) {
    String featureType = tileset.getFeatureType().orElse(tileset.getId());
    String geometryProperty =
        featureProvider
            .info()
            .getSchema(featureType)
            .flatMap(SchemaBase::getFilterGeometry)
            .map(SchemaBase::getFullPathAsString)
            .orElseThrow();
    Bbox bbox2 =
        Bbox.of(bbox.getXmin(), bbox.getYmin(), bbox.getXmax(), bbox.getYmax(), bbox.getEpsgCrs());
    Cql2Expression filter = SIntersects.of(Property.of(geometryProperty), SpatialLiteral.of(bbox2));

    if (additionalFilter.isPresent()) {
      filter = And.of(filter, additionalFilter.get());
    }

    FeatureQuery query =
        ImmutableFeatureQuery.builder()
            .type(featureType)
            .hitsOnly(true)
            .limit(1)
            .filter(filter)
            .build();

    FeatureStream featureStream = featureProvider.queries().get().getFeatureStream(query);

    CompletableFuture<CollectionMetadata> collectionMetadata = new CompletableFuture<>();

    featureStream.runWith(Sink.ignore(), Map.of(), collectionMetadata).toCompletableFuture().join();

    CollectionMetadata collectionMetadata1 = collectionMetadata.join();

    // LOGGER.debug("Feature query collection metadata: {} - {} - {}", featureType,
    // collectionMetadata1, filter);

    return collectionMetadata1.getNumberMatched().orElse(0) > 0L;
  }

  private static void setAvailability(
      byte[] availability, int startLevel, int level, int idxLevel) {
    int idx = idxLevel;
    // add shift from previous levels
    for (int i = startLevel; i < level; i++) {
      idx += MortonCode.size(i);
    }
    int byteIndex = idx / 8;
    int bitIndex = idx % 8;
    availability[byteIndex] |= 1 << bitIndex;
  }

  @SuppressWarnings("PMD.ExcessiveMethodLength")
  private static ImmutableSubtree buildSubtree(
      int subtreeLevels,
      int size,
      byte[] tileAvailability,
      byte[] contentAvailability,
      byte[] childSubtreeAvailability) {
    Boolean tileAvailabilityConstantValue = null;
    Boolean contentAvailabilityConstantValue = null;
    Boolean childSubtreeAvailabilityConstantValue = null;

    int tileAvailabilityCount = getAvailabilityCount(tileAvailability, 0, subtreeLevels);
    if (tileAvailabilityCount == 0) {
      tileAvailabilityConstantValue = false;
      contentAvailabilityConstantValue = false;
      childSubtreeAvailabilityConstantValue = false;
    } else if (tileAvailabilityCount == size) {
      tileAvailabilityConstantValue = true;
    }

    int contentAvailabilityCount = 0;
    int childSubtreeAvailabilityCount = 0;

    if (tileAvailabilityCount > 0) {
      contentAvailabilityCount = getAvailabilityCount(contentAvailability, 0, subtreeLevels);
      if (contentAvailabilityCount == 0) {
        contentAvailabilityConstantValue = false;
      } else if (contentAvailabilityCount == size) {
        contentAvailabilityConstantValue = true;
      }

      childSubtreeAvailabilityCount =
          getAvailabilityCount(childSubtreeAvailability, subtreeLevels, 1);
      if (childSubtreeAvailabilityCount == 0) {
        childSubtreeAvailabilityConstantValue = false;
      } else if (childSubtreeAvailabilityCount == MortonCode.size(subtreeLevels)) {
        childSubtreeAvailabilityConstantValue = true;
      }
    }

    ImmutableSubtree.Builder builder = ImmutableSubtree.builder();

    int length =
        (Objects.nonNull(tileAvailabilityConstantValue) ? 0 : tileAvailability.length)
            + (Objects.nonNull(contentAvailabilityConstantValue) ? 0 : contentAvailability.length)
            + (Objects.nonNull(childSubtreeAvailabilityConstantValue)
                ? 0
                : childSubtreeAvailability.length);
    builder.addBuffers(ImmutableBuffer.builder().byteLength(length).build());

    int bitstream = 0;
    int byteOffset = 0;
    if (Objects.nonNull(tileAvailabilityConstantValue)) {
      builder.tileAvailability(
          ImmutableAvailability.builder().constant(tileAvailabilityConstantValue ? 1 : 0).build());
    } else {
      builder
          .tileAvailability(getAvailability(bitstream++, tileAvailabilityCount))
          .addBufferViews(getBufferView(tileAvailability, byteOffset));
      byteOffset += tileAvailability.length;
    }
    if (Objects.nonNull(contentAvailabilityConstantValue)) {
      builder.contentAvailability(
          ImmutableList.of(
              ImmutableAvailability.builder()
                  .constant(contentAvailabilityConstantValue ? 1 : 0)
                  .build()));
    } else {
      builder
          .contentAvailability(
              ImmutableList.of(getAvailability(bitstream++, contentAvailabilityCount)))
          .addBufferViews(getBufferView(contentAvailability, byteOffset));
      byteOffset += contentAvailability.length;
    }
    if (Objects.nonNull(childSubtreeAvailabilityConstantValue)) {
      builder.childSubtreeAvailability(
          ImmutableAvailability.builder()
              .constant(childSubtreeAvailabilityConstantValue ? 1 : 0)
              .build());
    } else {
      builder
          .childSubtreeAvailability(getAvailability(bitstream, childSubtreeAvailabilityCount))
          .addBufferViews(getBufferView(childSubtreeAvailability, byteOffset));
    }

    if (Objects.isNull(tileAvailabilityConstantValue)) {
      builder.tileAvailabilityBin(tileAvailability);
    }

    if (Objects.isNull(contentAvailabilityConstantValue)) {
      builder.contentAvailabilityBin(contentAvailability);
    }

    if (Objects.isNull(childSubtreeAvailabilityConstantValue)) {
      builder.childSubtreeAvailabilityBin(childSubtreeAvailability);
    }

    // Unsupported options:
    // List<PropertyTable> getPropertyTables();
    // Optional<Integer> getTileMetadata();
    // List<Integer> getContentMetadata();
    // MetadataEntity getSubtreeMetadata();

    logAvailability(tileAvailability, contentAvailability, childSubtreeAvailability, subtreeLevels);

    return builder.build();
  }

  private static Optional<Polygon> computeExclusionPolygon(
      Tile3dCoordinates tile, BoundingBox bbox) {
    double dx = bbox.getXmax() - bbox.getXmin();
    double dy = bbox.getYmax() - bbox.getYmin();
    double factor = Math.pow(2, tile.getLevel());

    /* The exclusion polygon is the area west and north of the tile. If T is the tile,
      any building that intersects the tiles TW, TN, and TNW, the tiles to the west,
      north, and northwest of T, is excluded.
      Special cases are: In the northwest corner, there is no tile to the north or west,
      so no area is excluded. For the western or nothern boundaries, only the northern
      or western are excluded, respectively.

         x ---- x ---- x
         |      |      |
         | TNW  |  TN  |
         |      |      |
         x ---- x ---- x
         |      |      |
         |  TW  |  T   |
         |      |      |
         x ---- x ---- x
    */

    // handle the special cases first
    if (tile.getCol() == 0 && tile.getRow() == (factor - 1)) {
      return Optional.empty();
    } else if (tile.getCol() == 0) {
      double xmin = bbox.getXmin() + dx / factor * tile.getCol();
      double xmax = xmin + dx / factor;
      double ymin = bbox.getYmin() + dy / factor * (tile.getRow() + 1);
      double ymax = ymin + dy / factor;
      return Optional.of(Polygon.ofBbox(xmin, ymin, xmax, ymax, OgcCrs.CRS84));
    } else if (tile.getRow() == (factor - 1)) {
      double xmin = bbox.getXmin() + dx / factor * (tile.getCol() - 1);
      double xmax = xmin + dx / factor;
      double ymin = bbox.getYmin() + dy / factor * tile.getRow();
      double ymax = ymin + dy / factor;
      return Optional.of(Polygon.ofBbox(xmin, ymin, xmax, ymax, OgcCrs.CRS84));
    }

    double x0 = bbox.getXmin() + dx / factor * (tile.getCol() - 1);
    double y0 = bbox.getYmin() + dy / factor * tile.getRow();
    double x1 = x0 + dx / factor;
    double y1 = y0 + dy / factor;
    double x2 = x0 + 2 * dx / factor;
    double y2 = y0 + 2 * dy / factor;
    return Optional.of(
        Polygon.of(
            List.of(
                PositionList.of(
                    Axes.XY,
                    new double[] {x0, y0, x1, y0, x1, y1, x2, y1, x2, y2, x0, y2, x0, y0})),
            Optional.of(OgcCrs.CRS84)));
  }

  private static BufferView getBufferView(byte[] tileAvailability, int byteOffset) {
    return ImmutableBufferView.builder()
        .buffer(0)
        .byteOffset(byteOffset)
        .byteLength(tileAvailability.length)
        .build();
  }

  private static Availability getAvailability(int bitstream, int availabilityCount) {
    return ImmutableAvailability.builder()
        .bitstream(bitstream)
        .availabilityCount(availabilityCount)
        .build();
  }

  private static void logAvailability(
      byte[] tileAvailability,
      byte[] contentAvailability,
      byte[] childSubtreeAvailability,
      int subtreeLevels) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Tile Availability: {}", getAvailabilityString(tileAvailability, 0, subtreeLevels - 1));
      LOGGER.trace(
          "Content Availability: {}",
          getAvailabilityString(contentAvailability, 0, subtreeLevels - 1));
      LOGGER.trace(
          "Child Subtree Availability: {}",
          getAvailabilityString(childSubtreeAvailability, subtreeLevels, subtreeLevels));
    }
  }

  private static boolean getAvailability(
      byte[] availability, int startLevel, int level, int idxLevel) {
    int idx = idxLevel;
    // add shift from previous levels
    for (int i = startLevel; i < level; i++) {
      idx += MortonCode.size(i);
    }
    int byteIndex = idx / 8;
    int bitIndex = idx % 8;
    int bitValue = (availability[byteIndex] >> bitIndex) & 1;
    return bitValue == 1;
  }

  private static String getAvailabilityString(byte[] availability, int minLevel, int maxLevel) {
    StringBuilder s = new StringBuilder();
    for (int level = minLevel; level <= maxLevel; level++) {
      for (int i = 0; i < MortonCode.size(level); i++) {
        s.append(getAvailability(availability, minLevel, level, i) ? "1" : "0");
        if (i % 4 == 3) {
          s.append(' ');
        }
      }
      s.append(" / ");
    }
    return s.toString();
  }

  private static int getAvailabilityCount(byte[] availability, int minLevel, int levels) {
    int count = 0;
    for (int i = 0; i < levels; i++) {
      for (int j = 0; j < MortonCode.size(minLevel + i); j++) {
        if (getAvailability(availability, minLevel, minLevel + i, j)) {
          count++;
        }
      }
    }
    return count;
  }

  private static byte[] getBufferViewContent(
      byte[] subtreeBytes, Subtree subtreeWithEmptyBuffers, int bufferOffset, Integer i) {
    BufferView bv = subtreeWithEmptyBuffers.getBufferViews().get(i);
    checkBuffer(bv);
    final int offset = bufferOffset + bv.getByteOffset();
    return Arrays.copyOfRange(subtreeBytes, offset, offset + bv.getByteLength());
  }

  private static void checkBuffer(BufferView bv) {
    if (bv.getBuffer() != 0) {
      throw new IllegalStateException(
          String.format(
              "Invalid 3D Tiles subtree, only subtrees with a single buffer are supported. Found index: %d",
              bv.getBuffer()));
    }
  }

  private static boolean getAvailability(byte[] availability, int idx) {
    return isSet(availability, idx);
  }

  private static boolean getAvailability(byte[] availability, int level, int idxLevel) {
    int idx = idxLevel;
    // add shift from previous levels
    for (int i = 0; i < level; i++) {
      idx += MortonCode.size(i);
    }
    return isSet(availability, idx);
  }

  private static boolean isSet(byte[] availability, int idx) {
    int byteIndex = idx / 8;
    int bitIndex = idx % 8;
    int bitValue = (availability[byteIndex] >> bitIndex) & 1;
    return bitValue == 1;
  }

  private static byte[] intToLittleEndianLong(int v) {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(v);
    return bb.array();
  }

  private static int littleEndianIntToInt(byte[] array, int offset) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < 4; i++) {
      bb.put(array[offset + i]);
    }
    bb.rewind();
    return bb.getInt();
  }

  private static int littleEndianLongToInt(byte[] array, int offset) {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < 8; i++) {
      bb.put(array[offset + i]);
    }
    bb.rewind();
    return (int) bb.getLong();
  }
}
