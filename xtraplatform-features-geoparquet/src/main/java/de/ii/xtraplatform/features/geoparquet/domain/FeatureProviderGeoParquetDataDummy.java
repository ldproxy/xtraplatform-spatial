/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.domain;

import de.ii.xtraplatform.docs.DocMarker;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import javax.annotation.Nullable;

public interface FeatureProviderGeoParquetDataDummy {
  /**
   * @langEn See [Connection Info](#connection-info).
   * @langDe Siehe [Connection-Info](#connection-info).
   */
  @DocMarker("specific")
  @Nullable
  ConnectionInfoSql getConnectionInfo();
}
