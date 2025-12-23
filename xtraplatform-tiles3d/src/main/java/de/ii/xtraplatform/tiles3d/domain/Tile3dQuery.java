/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, attributeBuilderDetection = true)
public interface Tile3dQuery extends Tile3dCoordinates {
  Pattern IMPLICIT_FILE_NAME = Pattern.compile("^(\\d+)_(\\d+)_(\\d+)\\.(subtree|glb)$");

  String getTileset();

  Optional<String> getFileName();

  Optional<String> getType();

  Optional<Tile3dGenerationParameters> getGenerationParameters();

  @Value.Derived
  default boolean mayBeImplicit() {
    return getFileName().filter(name -> IMPLICIT_FILE_NAME.matcher(name).matches()).isPresent()
        && getGenerationParameters().isPresent();
  }

  @Value.Derived
  default boolean isSubtree() {
    return getType().filter("subtree"::equals).isPresent();
  }

  @Value.Derived
  default boolean isContent() {
    return getType().filter("glb"::equals).isPresent();
  }

  default Tile3dQuery toImplicit() {
    if (mayBeImplicit()) {
      Matcher matcher = IMPLICIT_FILE_NAME.matcher(getFileName().get());
      if (matcher.matches()) {
        int level = Integer.parseInt(matcher.group(1));
        int x = Integer.parseInt(matcher.group(2));
        int y = Integer.parseInt(matcher.group(3));
        String type = matcher.group(4);

        return ImmutableTile3dQuery.builder()
            .tileset(getTileset())
            .type(type)
            .level(level)
            .col(x)
            .row(y)
            .generationParameters(getGenerationParameters())
            .build();
      }
    }
    throw new IllegalStateException("Cannot convert to implicit Tile3dQuery");
  }

  default TileSubMatrix toTileSubMatrix() {
    return TileSubMatrix.of(getLevel(), getRow(), getRow(), getCol(), getCol());
  }
}
