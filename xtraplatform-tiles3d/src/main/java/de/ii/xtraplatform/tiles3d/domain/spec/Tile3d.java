/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3d.Builder.class)
public interface Tile3d {

  default Tile3d withUris(String uriPrefix) {
    return new ImmutableTile3d.Builder()
        .from(this)
        .content(
            getContent()
                .map(
                    content ->
                        new ImmutableWithUri.Builder()
                            .uri(uriPrefix + flattenUri(content.getUri()))
                            .build()))
        .children(getChildren().stream().map(child -> child.withUris(uriPrefix)).toList())
        .build();
  }

  @SuppressWarnings("UnstableApiUsage")
  Funnel<Tile3d> FUNNEL =
      (from, into) -> {
        BoundingVolume.FUNNEL.funnel(from.getBoundingVolume(), into);
        from.getGeometricError().ifPresent(into::putFloat);
        into.putString(from.getRefine(), StandardCharsets.UTF_8);
        from.getContent().ifPresent(content -> WithUri.FUNNEL.funnel(content, into));
        from.getImplicitTiling()
            .ifPresent(implicit -> ImplicitTiling.FUNNEL.funnel(implicit, into));
      };

  BoundingVolume getBoundingVolume();

  Optional<Float> getGeometricError();

  List<Double> getTransform();

  @Value.Default
  default String getRefine() {
    return "REPLACE";
  }

  Optional<WithUri> getContent();

  Optional<ImplicitTiling> getImplicitTiling();

  List<Tile3d> getChildren();

  default void accept(Tile3dVisitor visitor) {
    visitor.visit(this);
    getChildren().forEach(child -> child.accept(visitor));
  }

  static String flattenUri(String uri) {
    return uri.replaceAll("/", "_");
  }
}
