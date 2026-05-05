/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import jakarta.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Optional;

public interface DecoderFactories {

  Optional<Decoder> createDecoder(MediaType mediaType);

  Optional<FeatureDecoder<byte[]>> createFeatureDecoder(MediaType mediaType);

  Optional<Decoder> createGeometryDecoder(MediaType mediaType);

  Map<String, DecoderFactory> getConnectorDecoders();
}
