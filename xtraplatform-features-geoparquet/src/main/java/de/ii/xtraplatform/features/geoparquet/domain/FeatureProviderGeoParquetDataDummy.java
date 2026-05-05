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
