/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
import de.ii.xtraplatform.base.domain.util.Tuple;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.MediaType;

// TODO: only for byte decoders?
@AutoMultiBind
public interface DecoderFactory {

  @AutoMultiBind
  interface GeometryDecoderFactory {

    MediaType getGeometryMediaType();

    Decoder createGeometryDecoder();
  }

  @AutoMultiBind
  interface FeatureDecoderFactory {

    MediaType getMediaType();

    FeatureDecoder<byte[]> createFeatureDecoder();
  }

  // application/sql, application/x.wkt
  MediaType getMediaType();

  Decoder createDecoder();

  default Optional<String> getConnectorString() {
    return Optional.empty();
  }

  default de.ii.xtraplatform.base.domain.util.Tuple<String, String> parseSourcePath(
      String path, String column, String flags, String connectorSpec) {
    return Tuple.of(column, "");
  }

  default List<String> resolvePath(List<String> path) {
    return path;
  }
}
