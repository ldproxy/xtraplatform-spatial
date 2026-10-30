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
import de.ii.xtralink.jobs.Identifiers.ProgressOperation;
import de.ii.xtralink.jobs.PartialJobConfiguration;
import de.ii.xtralink.jobs.ProgressUpdate;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.xtralink.domain.JobContext;
import de.ii.xtraplatform.xtralink.domain.Jobs;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3dSeedingPartialJob.Builder.class)
public interface Tile3dSeedingPartialJob extends JobContext {

  String TYPE_SUBTREE = Tile3dSeedingJob.type("subtree", "binary");
  String TYPE_GLTF = Tile3dSeedingJob.type("content", "glb");

  @SuppressWarnings("PMD.DataClass")
  final class Context {
    private final String tileProvider;
    private final String tileSet;
    private final String tileMatrixSet;
    private final Optional<Tile3dGenerationParameters> generationParameters;
    private final String jobId;

    public Context(
        String tileProvider,
        String tileSet,
        String tileMatrixSet,
        Optional<Tile3dGenerationParameters> generationParameters,
        String jobId) {
      this.tileProvider = tileProvider;
      this.tileSet = tileSet;
      this.tileMatrixSet = tileMatrixSet;
      this.generationParameters = generationParameters;
      this.jobId = jobId;
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

    String getJobId() {
      return jobId;
    }
  }

  // Content needs the subtree above it to exist, so the partial jobs below a subtree are only
  // pushed once that subtree was seeded.
  static PartialJobConfiguration subtree(
      int priority,
      Context context,
      boolean isReseed,
      TileSubMatrix subMatrix,
      List<PartialJobConfiguration> followUps) {
    ImmutableTile3dSeedingPartialJob details =
        new ImmutableTile3dSeedingPartialJob.Builder()
            .tileProvider(context.getTileProvider())
            .tileSet(context.getTileSet())
            .tileMatrixSet(context.getTileMatrixSet())
            .generationParameters(context.getGenerationParameters())
            .encoding(MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .isReseed(isReseed)
            .addSubMatrices(subMatrix)
            .followUps(followUps)
            .build();

    return Jobs.createPartial(
        TYPE_SUBTREE,
        priority,
        context.getJobId(),
        details,
        (int) details.getNumberOfTiles(),
        progressUpdates(details));
  }

  static PartialJobConfiguration content(
      int priority, Context context, boolean isReseed, TileSubMatrix subMatrix) {
    ImmutableTile3dSeedingPartialJob details =
        new ImmutableTile3dSeedingPartialJob.Builder()
            .tileProvider(context.getTileProvider())
            .tileSet(context.getTileSet())
            .tileMatrixSet(context.getTileMatrixSet())
            .generationParameters(context.getGenerationParameters())
            .encoding(new MediaType("model", "gltf-binary"))
            .isReseed(isReseed)
            .addSubMatrices(subMatrix)
            .build();

    return Jobs.createPartial(
        TYPE_GLTF,
        priority,
        context.getJobId(),
        details,
        (int) details.getNumberOfTiles(),
        progressUpdates(details));
  }

  // The queue applies the whole delta to every path, so per-level counts are only correct for a
  // single submatrix.
  private static List<ProgressUpdate> progressUpdates(Tile3dSeedingPartialJob details) {
    if (details.getSubMatrices().size() != 1) {
      throw new IllegalArgumentException("3D tile seeding partial jobs need exactly one submatrix");
    }
    String tileSet = details.getTileSet();
    int level = details.getSubMatrices().get(0).getLevel();

    return List.of(
        new ProgressUpdate(String.format("%s.current", tileSet), ProgressOperation.ADD),
        new ProgressUpdate(
            String.format("%s.levels.%s[%d]", tileSet, details.getTileMatrixSet(), level),
            ProgressOperation.SUBTRACT));
  }

  String getTileProvider();

  String getTileSet();

  Optional<Tile3dGenerationParameters> getGenerationParameters();

  String getTileMatrixSet();

  MediaType getEncoding();

  boolean isReseed();

  Map<String, String> getStorage();

  List<TileSubMatrix> getSubMatrices();

  List<PartialJobConfiguration> getFollowUps();

  @Value.Derived
  @Value.Auxiliary
  @JsonIgnore
  default long getNumberOfTiles() {
    return getSubMatrices().stream().mapToLong(TileSubMatrix::getNumberOfTiles).sum();
  }
}
