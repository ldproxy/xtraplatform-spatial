/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtralink.jobs.Identifiers.ProgressOperation;
import de.ii.xtralink.jobs.PartialJobConfiguration;
import de.ii.xtralink.jobs.ProgressUpdate;
import de.ii.xtraplatform.tiles.app.FeatureEncoderMVT;
import de.ii.xtraplatform.xtralink.domain.JobContext;
import de.ii.xtraplatform.xtralink.domain.Jobs;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileSeedingPartialJob.Builder.class)
public interface TileSeedingPartialJob extends JobContext {

  String TYPE_MVT = TileSeedingJob.type("vector", "mvt");
  String TYPE_PNG = TileSeedingJob.type("raster", "png");

  static PartialJobConfiguration of(
      int priority,
      String tileProvider,
      String tileSet,
      String tileMatrixSet,
      boolean isReseed,
      Set<TileSubMatrix> subMatrices,
      Optional<TileGenerationParameters> generationParameters,
      String jobSetId) {
    ImmutableTileSeedingPartialJob details =
        new ImmutableTileSeedingPartialJob.Builder()
            .tileProvider(tileProvider)
            .tileSet(tileSet)
            .tileMatrixSet(tileMatrixSet)
            .generationParameters(generationParameters)
            .encoding(FeatureEncoderMVT.FORMAT)
            .isReseed(isReseed)
            .addAllSubMatrices(subMatrices)
            .build();

    // TODO: only works with single submatrix, needs to be fixed for multiple submatrices
    List<ProgressUpdate> progressUpdates =
        List.of(
            // new ProgressUpdate(String.format("%s.current", tileSet), ProgressOperation.ADD),
            new ProgressUpdate(
                String.format(
                    "%s.%s[%d]",
                    tileSet, details.getTileMatrixSet(), subMatrices.iterator().next().getLevel()),
                ProgressOperation.SUBTRACT));

    return Jobs.createPartial(
        TYPE_MVT, priority, jobSetId, details, (int) details.getNumberOfTiles(), progressUpdates);
  }

  static PartialJobConfiguration raster(
      int priority,
      String tileProvider,
      String tileSet,
      String tileMatrixSet,
      boolean isReseed,
      Set<TileSubMatrix> subMatrices,
      String jobSetId,
      Map<String, String> storageInfo) {
    ImmutableTileSeedingPartialJob details =
        new ImmutableTileSeedingPartialJob.Builder()
            .tileProvider(tileProvider)
            .tileSet(tileSet)
            .tileMatrixSet(tileMatrixSet)
            .encoding(MediaType.valueOf("image/png"))
            .isReseed(isReseed)
            .addAllSubMatrices(subMatrices)
            .storage(storageInfo)
            .build();

    // TODO: only works with single submatrix, needs to be fixed for multiple submatrices
    List<ProgressUpdate> progressUpdates =
        List.of(
            new ProgressUpdate(
                String.format(
                    "%s.%s[%d]",
                    tileSet, details.getTileMatrixSet(), subMatrices.iterator().next().getLevel()),
                ProgressOperation.SUBTRACT));

    return Jobs.createPartial(
        TYPE_PNG, priority, jobSetId, details, (int) details.getNumberOfTiles(), progressUpdates);
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
