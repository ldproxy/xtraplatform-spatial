/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.DecoderFactory;
import de.ii.xtraplatform.features.json.domain.DecoderJson;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;

@Singleton
@AutoBind
@SuppressWarnings("PMD.UseObjectForClearerAPI")
public class DecoderFactoryJson implements DecoderFactory {

  @Inject
  public DecoderFactoryJson() {}

  @Override
  public MediaType getMediaType() {
    return MediaType.APPLICATION_JSON_TYPE;
  }

  @Override
  public Optional<String> getConnectorString() {
    return Optional.of("JSON");
  }

  @Override
  public Decoder createDecoder() {
    return new DecoderJson(Optional.empty());
  }

  @Override
  public Tuple<String, String> parseSourcePath(
      String path, String column, String flags, String connectorSpec) {
    String pathInConnector = path.substring(path.indexOf(connectorSpec) + connectorSpec.length());

    if (!pathInConnector.isEmpty()) {
      return Tuple.of(column, pathInConnector.substring(1).replace('/', '.'));
    }

    return Tuple.of(column, "");
  }
}
