/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra;

import de.ii.xtraplatform.features.domain.ConnectorFactory;
import de.ii.xtraplatform.features.gml.domain.ConnectionInfoWfsHttp;
import de.ii.xtraplatform.features.gml.domain.WfsClientBasic;
import de.ii.xtraplatform.features.gml.domain.WfsClientBasicFactory;
import de.ii.xtraplatform.features.gml.domain.WfsConnector;

public class WfsClientBasicFactoryDefault implements WfsClientBasicFactory {
  private final ConnectorFactory connectorFactory;

  public WfsClientBasicFactoryDefault(ConnectorFactory connectorFactory) {
    this.connectorFactory = connectorFactory;
  }

  @Override
  public WfsClientBasic create(
      String providerType, String providerId, ConnectionInfoWfsHttp connectionInfo) {
    WfsConnector connector =
        (WfsConnector) connectorFactory.createConnector(providerType, providerId, connectionInfo);

    if (!connector.isConnected()) {
      connectorFactory.disposeConnector(connector);

      RuntimeException connectionError =
          connector
              .getConnectionError()
              .map(
                  throwable ->
                      throwable instanceof RuntimeException
                          ? (RuntimeException) throwable
                          : new RuntimeException(throwable))
              .orElse(new IllegalStateException("unknown reason"));

      throw connectionError;
    }

    return connector;
  }

  @Override
  public void dispose(WfsClientBasic wfsClient) {
    if (wfsClient instanceof WfsConnector) {
      connectorFactory.disposeConnector((WfsConnector) wfsClient);
    }
  }
}
