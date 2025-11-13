/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.entities.domain.AbstractPersistentEntity;
import de.ii.xtraplatform.tiles3d.domain.Tile3dAccess;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderData;
import de.ii.xtraplatform.tiles3d.domain.spec.Tileset3d;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTile3dProvider<T extends Tile3dProviderData>
    extends AbstractPersistentEntity<T> implements Tile3dProvider, Tile3dAccess {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTile3dProvider.class);

  public AbstractTile3dProvider(VolatileRegistry volatileRegistry, T data, String... capabilities) {
    super(data, volatileRegistry, capabilities);
  }

  @Override
  protected boolean onStartup() throws InterruptedException {
    onVolatileStart();

    return super.onStartup();
  }

  @Override
  protected void onStarted() {
    super.onStarted();

    onStateChange(
        (from, to) -> {
          LOGGER.info("3dTile provider with id '{}' state changed: {}", getId(), getState());
        },
        true);

    LOGGER.info("3dTile provider with id '{}' started successfully.", getId());
  }

  @Override
  protected void onReloaded(boolean forceReload) {
    LOGGER.info("3dTile provider with id '{}' reloaded successfully.", getId());
  }

  @Override
  protected void onStopped() {
    LOGGER.info("3dTile provider with id '{}' stopped.", getId());
  }

  @Override
  protected void onStartupFailure(Throwable throwable) {
    LogContext.error(
        LOGGER, throwable, "3dTile provider with id '{}' could not be started", getId());
  }

  @Override
  public Optional<Tileset3d> getMetadata(String tilesetId) {
    return Optional.empty();
  }
}
