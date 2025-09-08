/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * @langEn ### Seeding
 *     <p>Controls how and when [caches](#cache) are computed.
 * @langDe ### Seeding
 *     <p>Steuert wie und wann [Caches](#cache) berechnet werden.
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableSeedingOptions.Builder.class)
public interface SeedingOptions {

  enum JobSize {
    S,
    M,
    L,
    XL;

    public int getNumberOfTiles() {
      switch (this) {
        case S:
          return 256;
        case M:
          return 1024;
        case L:
          return 16384;
        case XL:
          return 65536;
        default:
          return 1024;
      }
    }
  }

  /**
   * @langEn If disabled, the seeding will not run when the API starts.
   * @langDe Steuert, ob das Seeding beim Start einer API ausgeführt wird.
   * @default true
   */
  @Nullable
  Boolean getRunOnStartup();

  @Value.Lazy
  @JsonIgnore
  default boolean shouldRunOnStartup() {
    return !Objects.equals(getRunOnStartup(), false);
  }

  /**
   * @langEn A crontab pattern to run the seeding periodically. There will only ever be one seeding
   *     in progress, so if the next run is scheduled before the last one finished, it will be
   *     skipped.
   * @langDe Ein Crontab-Pattern für die regelmäßige Ausführung des Seedings. Das Seeding wird stets
   *     nur einmal pro API zur gleichen Zeit ausgeführt, d.h. falls eine weitere Ausführung
   *     ansteht, während die vorherige noch läuft, wird diese übersprungen.
   * @default null
   */
  @Nullable
  String getRunPeriodic();

  @Value.Lazy
  @JsonIgnore
  default boolean shouldRunPeriodic() {
    return Objects.nonNull(getRunPeriodic());
  }

  /**
   * @langEn If disabled, the seeding will not run on dataset change events. See [Dataset
   *     Changes](../feature/10-sql.md#dataset-changes).
   * @langDe Steuert, ob das Seeding bei Dataset Change Events ausgeführt wird. Siehe
   *     [Datensatzänderungen](../feature/10-sql.md#dataset-changes).
   * @since v4.3
   * @default true
   */
  @Nullable
  Boolean getRunOnDatasetChange();

  @Value.Lazy
  @JsonIgnore
  default boolean shouldRunOnDatasetChange() {
    return !Objects.equals(getRunOnDatasetChange(), false);
  }

  @Value.Lazy
  @JsonIgnore
  default Optional<String> getCronExpression() {
    return Optional.ofNullable(getRunPeriodic());
  }

  /**
   * @langEn If enabled the tile cache will be purged before the seeding starts.
   * @langDe Steuert, ob der Cache vor dem Seeding bereinigt wird.
   * @default false
   */
  @Nullable
  Boolean getPurge();

  @Value.Lazy
  @JsonIgnore
  default boolean shouldPurge() {
    return Objects.equals(getPurge(), true);
  }

  /**
   * @langEn *Deprecated* This option has no effect anymore. All available background task threads
   *     will be used for seeding. You can control the order of execution by setting the `priority`
   *     of the seeding job.
   * @langDe *Deprecated* Diese Option hat keine Auswirkung mehr. Alle verfügaren Threads für
   *     Hintergrundprozesse werden für das Seeding verwendet. Die Reihenfolge der Ausführung kann
   *     über die `priority` des Seeding-Jobs gesteuert werden.
   * @default 1
   */
  @Deprecated(since = "v4.5", forRemoval = true)
  @Nullable
  Integer getMaxThreads();

  @Deprecated(since = "v4.5", forRemoval = true)
  @Value.Lazy
  @JsonIgnore
  default int getEffectiveMaxThreads() {
    return Objects.isNull(getMaxThreads()) || getMaxThreads() <= 1 ? 1 : getMaxThreads();
  }

  /**
   * @langEn The maximum number of tiles in a seeding job (S=256, M=1024, L=16384, XL=65536). The
   *     tile seeding is split into multiple jobs to distribute the work across threads and nodes.
   * @langDe Die maximale Anzahl an Tiles in einem Seeding-Job (S=256, M=1024, L=16384, XL=65536).
   *     Das Seeding wird in mehrere Jobs aufgeteilt, um die Arbeit auf Threads und Knoten zu
   *     verteilen.
   * @default M
   */
  @Nullable
  JobSize getJobSize();

  @Value.Lazy
  @JsonIgnore
  default int getEffectiveJobSize() {
    return Objects.requireNonNullElse(getJobSize(), JobSize.M).getNumberOfTiles();
  }

  /**
   * @langEn The priority of the seeding job. This controls the order in which this seeding job is
   *     executed compared to other seeding jobs. A higher number means that the seeding job is
   *     executed earlier.
   * @langDe Die Priorität des Seeding-Jobs. Diese steuert die Reihenfolge, in der dieser
   *     Seeding-Job im Vergleich zu anderen Seeding-Jobs ausgeführt wird. Eine höhere Zahl
   *     bedeutet, dass der Seeding-Job früher ausgeführt wird.
   * @since v4.5
   * @default 1000
   */
  @Nullable
  Integer getPriority();

  @Value.Lazy
  @JsonIgnore
  default int getEffectivePriority() {
    return Objects.requireNonNullElse(getPriority(), 1000);
  }
}
