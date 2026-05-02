/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.jobs.domain.Job;
import de.ii.xtraplatform.jobs.domain.Job.JobDetails;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import jakarta.ws.rs.core.MediaType;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3dSeedingJob.Builder.class)
public interface Tile3dSeedingJob extends JobDetails {

  String TYPE_SUBTREE = Tile3dSeedingJobSet.type("subtree", "binary");
  String TYPE_GLTF = Tile3dSeedingJobSet.type("content", "glb");

  @SuppressWarnings("PMD.DataClass")
  final class Context {
    private final String tileProvider;
    private final String tileSet;
    private final String tileMatrixSet;
    private final Optional<Tile3dGenerationParameters> generationParameters;
    private final String jobSetId;

    public Context(
        String tileProvider,
        String tileSet,
        String tileMatrixSet,
        Optional<Tile3dGenerationParameters> generationParameters,
        String jobSetId) {
      this.tileProvider = tileProvider;
      this.tileSet = tileSet;
      this.tileMatrixSet = tileMatrixSet;
      this.generationParameters = generationParameters;
      this.jobSetId = jobSetId;
    }

    String getTileProvider() {
      return tileProvider;
    }

    String getTileSet() {
      return tileSet;
    }

    String getTileMatrixSet() {
      return tileMatrixSet;
    }

    Optional<Tile3dGenerationParameters> getGenerationParameters() {
      return generationParameters;
    }

    String getJobSetId() {
      return jobSetId;
    }
  }

  static Job subtree(
      int priority, Context context, boolean isReseed, Set<TileSubMatrix> subMatrices) {
    ImmutableTile3dSeedingJob details =
        new ImmutableTile3dSeedingJob.Builder()
            .tileProvider(context.getTileProvider())
            .tileSet(context.getTileSet())
            .tileMatrixSet(context.getTileMatrixSet())
            .generationParameters(context.getGenerationParameters())
            .encoding(MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .isReseed(isReseed)
            .addAllSubMatrices(subMatrices)
            .build();

    return Job.of(
        TYPE_SUBTREE, priority, details, context.getJobSetId(), (int) details.getNumberOfTiles());
  }

  static Job content(
      int priority, Context context, boolean isReseed, Set<TileSubMatrix> subMatrices) {
    ImmutableTile3dSeedingJob details =
        new ImmutableTile3dSeedingJob.Builder()
            .tileProvider(context.getTileProvider())
            .tileSet(context.getTileSet())
            .tileMatrixSet(context.getTileMatrixSet())
            .generationParameters(context.getGenerationParameters())
            .encoding(new MediaType("model", "gltf-binary"))
            .isReseed(isReseed)
            .addAllSubMatrices(subMatrices)
            .build();

    return Job.of(
        TYPE_GLTF, priority, details, context.getJobSetId(), (int) details.getNumberOfTiles());
  }

  String getTileProvider();

  String getTileSet();

  Optional<Tile3dGenerationParameters> getGenerationParameters();

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
