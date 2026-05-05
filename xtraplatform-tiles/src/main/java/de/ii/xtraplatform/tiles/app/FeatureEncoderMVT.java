/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.transform.FeatureEncoderSfFlat;
import de.ii.xtraplatform.features.domain.transform.FeatureSfFlat;
import de.ii.xtraplatform.tiles.domain.LevelTransformation;
import de.ii.xtraplatform.tiles.domain.TileBuilder;
import de.ii.xtraplatform.tiles.domain.TileCoordinates;
import de.ii.xtraplatform.tiles.domain.TileGenerationContext;
import de.ii.xtraplatform.tiles.domain.TileGenerationOptions;
import jakarta.ws.rs.core.MediaType;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import no.ecc.vectortile.VectorTileEncoder;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureEncoderMVT extends FeatureEncoderSfFlat {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureEncoderMVT.class);
  public static final MediaType FORMAT = new MediaType("application", "vnd.mapbox-vector-tile");
  public static final byte[] EMPTY_TILE = new VectorTileEncoder().encode();

  private final TileGenerationOptions parameters;
  private final TileCoordinates tile;
  private final VectorTileEncoder tileEncoder;
  private final AffineTransformation affineTransformation;
  private final String tileset;
  private final PrecisionModel tilePrecisionModel;
  private final GeometryFactory geometryFactoryTile;
  private final GeometryFactory geometryFactoryWorld;
  private final Polygon clipGeometry;
  private final List<String> groupBy;
  private final Set<MvtFeature> mergeFeatures;

  private long mergeCount = 0;
  private long featureCount = 0;
  private boolean full = true;

  public FeatureEncoderMVT(TileGenerationContext encodingContext) {
    super(encodingContext);
    this.parameters = encodingContext.getParameters();
    this.tile = encodingContext.getCoordinates();
    this.tileEncoder = new VectorTileEncoder(tile.getTileMatrixSet().getTileExtent());
    this.affineTransformation = createTransformNativeToTile();
    this.tileset = encodingContext.getTileset();
    this.tilePrecisionModel =
        new PrecisionModel(
            (double) tile.getTileMatrixSet().getTileExtent()
                / (double) tile.getTileMatrixSet().getTileSize());
    this.geometryFactoryTile = new GeometryFactory(tilePrecisionModel);
    this.geometryFactoryWorld = new GeometryFactory();

    final int size = tile.getTileMatrixSet().getTileSize();
    final int buffer = TileBuilder.BUFFER_SIZE_FORMAL;
    CoordinateXY[] coords = new CoordinateXY[5];
    coords[0] = new CoordinateXY(-buffer, size + buffer);
    coords[1] = new CoordinateXY(size + buffer, size + buffer);
    coords[2] = new CoordinateXY(size + buffer, -buffer);
    coords[3] = new CoordinateXY(-buffer, -buffer);
    coords[4] = coords[0];
    this.clipGeometry = geometryFactoryTile.createPolygon(coords);

    final Map<String, List<LevelTransformation>> rules = parameters.getTransformations();
    this.groupBy =
        (Objects.nonNull(rules) && rules.containsKey(tile.getTileMatrixSet().getId()))
            ? rules.get(tile.getTileMatrixSet().getId()).stream()
                .filter(
                    rule ->
                        rule.getMax() >= tile.getLevel()
                            && rule.getMin() <= tile.getLevel()
                            && rule.getMerge().orElse(false))
                .map(LevelTransformation::getGroupBy)
                .findAny()
                .orElse(null)
            : null;
    this.mergeFeatures = new HashSet<>();
  }

  @Override
  public void onStart(ModifiableContext context) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Start generating tile for tileset {}, tile {}/{}/{}/{}.",
          tileset,
          tile.getTileMatrixSet().getId(),
          tile.getLevel(),
          tile.getRow(),
          tile.getCol());
    }
    this.processingStart = System.nanoTime();
  }

  @Override
  public void onFeature(FeatureSfFlat feature) {
    long startFeature = System.nanoTime();
    featureCount++;

    Optional<Geometry> featureGeometry = feature.getJtsGeometry(geometryFactoryWorld);

    if (featureGeometry.isEmpty()) {
      return;
    }

    try {
      Geometry tileGeometry =
          TileGeometryUtil.getTileGeometry(
              featureGeometry.get(),
              affineTransformation,
              clipGeometry,
              tilePrecisionModel,
              parameters.getMinimumSizeInPixel());
      if (Objects.isNull(tileGeometry)) {
        return;
      }

      // in "full" tiles all features cover then whole tile
      try {
        full = full && tileGeometry.equals(clipGeometry);
      } catch (Exception ignore) {
      }

      // if polygons have to be merged, store them for now and process at the end
      if (Objects.nonNull(groupBy) && tileGeometry.getGeometryType().contains("Polygon")) {
        mergeFeatures.add(
            new ImmutableMvtFeature.Builder()
                .id(++mergeCount)
                .properties(feature.getPropertiesAsMap())
                .geometry(tileGeometry)
                .build());
        return;
      }

      // Geometry is invalid -> try to fix the geometry, otherwise log this information and skip it,
      // if that option is used
      if (!tileGeometry.isValid()) {
        tileGeometry = new GeometryFixer(tileGeometry).getResult();
        if (!tileGeometry.isValid()) {
          LOGGER.warn(
              "Feature {} in tileset {} has an invalid tile geometry in tile {}/{}/{}/{}. Size in pixels: {}.",
              feature.getIdValue(),
              tileset,
              tile.getTileMatrixSet().getId(),
              tile.getLevel(),
              tile.getRow(),
              tile.getCol(),
              featureGeometry.get().getArea());
          if (Boolean.TRUE.equals(parameters.getIgnoreInvalidGeometries())) {
            return;
          }
        }
      }

      // If we have an id that happens to be a long value, use it
      Long id = null;
      if (feature.getIdValue() != null) {
        try {
          id = Long.parseLong(feature.getIdValue());
        } catch (Exception e) {
          // nothing to do
        }
      }

      // Add the feature with the layer name, a Map with attributes and the JTS Geometry.
      if (Objects.nonNull(id)) {
        tileEncoder.addFeature(tileset, feature.getPropertiesAsMap(), tileGeometry, id);
      } else {
        tileEncoder.addFeature(tileset, feature.getPropertiesAsMap(), tileGeometry);
      }
      written++;

    } catch (Exception e) {
      LOGGER.error(
          "Error while processing feature {} in tile {}/{}/{}/{} in tileset {}. The feature is skipped. Reason: {}",
          feature.getIdValue(),
          tile.getTileMatrixSet().getId(),
          tile.getLevel(),
          tile.getRow(),
          tile.getCol(),
          tileset,
          e.getMessage());
      if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
        LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace:", e);
      }
    }
    featureDuration += System.nanoTime() - startFeature;
  }

  @Override
  public void onEnd(ModifiableContext context) {
    long mergerStart = System.nanoTime();
    if (Objects.nonNull(groupBy) && mergeCount > 0) {
      FeatureMerger merger =
          new FeatureMerger(
              groupBy,
              allProperties,
              properties,
              geometryFactoryTile,
              tilePrecisionModel,
              String.format(
                  "Tileset %s, tile %s/%d/%d/%d",
                  tileset,
                  tile.getTileMatrixSet().getId(),
                  tile.getLevel(),
                  tile.getRow(),
                  tile.getCol()));
      merger
          .merge(mergeFeatures)
          .forEach(
              mergedFeature -> {
                Geometry geom = mergedFeature.getGeometry();
                // Geometry is invalid? -> try to fix the geometry, otherwise log this information
                // and skip it, if that option is used
                if (!geom.isValid()) {
                  geom = new GeometryFixer(geom).getResult();
                  if (!geom.isValid()) {
                    LOGGER.warn(
                        "A merged feature in tileset {} has an invalid tile geometry in tile {}/{}/{}/{}. Properties: {}",
                        tileset,
                        tile.getTileMatrixSet().getId(),
                        tile.getLevel(),
                        tile.getRow(),
                        tile.getCol(),
                        mergedFeature.getProperties());
                    if (Boolean.TRUE.equals(parameters.getIgnoreInvalidGeometries())) {
                      return;
                    }
                  }
                }
                tileEncoder.addFeature(tileset, mergedFeature.getProperties(), geom);
                written++;
              });
    }
    long mergerDuration = (System.nanoTime() - mergerStart) / 1_000_000;

    long encoderStart = System.nanoTime();

    byte[] mvt = tileEncoder.encode();
    push(mvt);

    if (featureCount == 0) {
      // TODO header/trailer/field "OATiles-hint: empty", also include info in tile cache
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "Tileset {}, tile {}/{}/{}/{} is empty.",
            tileset,
            tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol());
      }
    } else if (featureCount == written && full) {
      // TODO header/trailer/field "OATiles-hint: full", also include info in tile cache
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "Tileset {}, tile {}/{}/{}/{} is full.",
            tileset,
            tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol());
      }
    }

    if (LOGGER.isDebugEnabled()) {
      long encoderDuration = (System.nanoTime() - encoderStart) / 1_000_000;
      long transformerDuration = (System.nanoTime() - transformerStart) / 1_000_000;
      long processingDuration = (System.nanoTime() - processingStart) / 1_000_000;
      int kiloBytes = mvt.length / 1024;
      String text =
          String.format(
              "Tileset %s, tile %s/%d/%d/%d written. Features returned: %d, written: %d, total duration: %dms, processing: %dms, feature processing: %dms, merging: %dms, encoding: %dms, size: %dkB.",
              tileset,
              tile.getTileMatrixSet().getId(),
              tile.getLevel(),
              tile.getRow(),
              tile.getCol(),
              context.metadata().getNumberReturned().orElse(0),
              written,
              transformerDuration,
              processingDuration,
              featureDuration / 1_000_000,
              mergerDuration,
              encoderDuration,
              kiloBytes);

      if (processingDuration > 1000 || kiloBytes > 250) {
        LOGGER.debug(text);
      } else if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(text);
      }
    }
  }

  private AffineTransformation createTransformNativeToTile() {

    BoundingBox bbox = tile.getBoundingBox();

    double xMin = bbox.getXmin();
    double xMax = bbox.getXmax();
    double yMin = bbox.getYmin();
    double yMax = bbox.getYmax();

    double tileSize = tile.getTileMatrixSet().getTileSize();

    double xScale = tileSize / (xMax - xMin);
    double yScale = tileSize / (yMax - yMin);

    double xOffset = -xMin * xScale;
    double yOffset = yMin * yScale + tileSize;

    return new AffineTransformation(xScale, 0.0d, xOffset, 0.0d, -yScale, yOffset);
  }
}
