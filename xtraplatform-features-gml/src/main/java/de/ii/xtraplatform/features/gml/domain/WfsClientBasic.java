/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import de.ii.xtraplatform.features.domain.Metadata;
import de.ii.xtraplatform.features.gml.infra.req.WfsOperation;
import java.io.InputStream;
import java.util.Optional;

public interface WfsClientBasic {
  InputStream runWfsOperation(WfsOperation operation);

  Optional<Metadata> getMetadata();

  Optional<Throwable> getConnectionError();
}
