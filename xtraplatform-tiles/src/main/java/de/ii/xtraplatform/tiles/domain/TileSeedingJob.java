/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.ii.xtraplatform.jobs.domain.Job;
import de.ii.xtraplatform.tiles.app.FeatureEncoderMVT;
import de.ii.xtraplatform.tiles.domain.ImmutableTileSeedingJob.Builder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.MediaType;
import org.immutables.value.Value;

@Value.Immutable
public interface TileSeedingJob {

  String TYPE_MVT = TileSeedingJobSet.type("vector", "mvt");
  String TYPE_PNG = TileSeedingJobSet.type("raster", "png");

  static Job of(
      String tileProvider,
      String tileSet,
      String tileMatrixSet,
      boolean isReseed,
      Set<TileSubMatrix> subMatrices,
      Optional<TileGenerationParameters> generationParameters,
      String jobSetId) {
    ImmutableTileSeedingJob details =
        new Builder()
            .tileProvider(tileProvider)
            .tileSet(tileSet)
            .tileMatrixSet(tileMatrixSet)
            .generationParameters(generationParameters)
            .encoding(FeatureEncoderMVT.FORMAT)
            .isReseed(isReseed)
            .addAllSubMatrices(subMatrices)
            .build();

    return Job.of(TYPE_MVT, details, jobSetId, (int) details.getNumberOfTiles());
  }

  static Job raster(
      String tileProvider,
      String tileSet,
      String tileMatrixSet,
      boolean isReseed,
      Set<TileSubMatrix> subMatrices,
      String jobSetId,
      Map<String, String> storageInfo) {
    ImmutableTileSeedingJob details =
        new Builder()
            .tileProvider(tileProvider)
            .tileSet(tileSet)
            .tileMatrixSet(tileMatrixSet)
            .encoding(MediaType.valueOf("image/png"))
            .isReseed(isReseed)
            .addAllSubMatrices(subMatrices)
            .storage(storageInfo)
            .build();

    return Job.of(TYPE_PNG, details, jobSetId, (int) details.getNumberOfTiles());
  }

  String getTileProvider();

  String getTileSet();

  Optional<TileGenerationParameters> getGenerationParameters();

  String getTileMatrixSet();

  MediaType getEncoding();

  boolean isReseed();

  Map<String, String> getStorage();

  List<TileSubMatrix> getSubMatrices();

  @Value.Derived
  @Value.Auxiliary
  @JsonIgnore
  default long getNumberOfTiles() {
    return getSubMatrices().stream().mapToLong(TileSubMatrix::getNumberOfTiles).sum();
  }
}
