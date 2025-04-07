/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import de.ii.xtraplatform.features.domain.DatasetChange;
import de.ii.xtraplatform.features.domain.DatasetChangeListener;
import de.ii.xtraplatform.features.domain.FeatureChange;
import de.ii.xtraplatform.features.domain.FeatureChangeListener;
import de.ii.xtraplatform.features.domain.FeatureChanges;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureChangeHandlerImpl implements FeatureChanges {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureChangeHandlerImpl.class);

  private final List<DatasetChangeListener> datasetListeners;
  private final List<FeatureChangeListener> featureListeners;

  public FeatureChangeHandlerImpl() {
    this.datasetListeners = new CopyOnWriteArrayList<>();
    this.featureListeners = new CopyOnWriteArrayList<>();
  }

  @Override
  public void addListener(DatasetChangeListener listener) {
    datasetListeners.add(listener);
  }

  @Override
  public void addListener(FeatureChangeListener listener) {
    featureListeners.add(listener);
  }

  @Override
  public void removeListener(DatasetChangeListener listener) {
    datasetListeners.remove(listener);
  }

  @Override
  public void removeListener(FeatureChangeListener listener) {
    featureListeners.remove(listener);
  }

  @Override
  public void handle(DatasetChange change) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Handling dataset change: {}", change);
    }

    datasetListeners.forEach(
        listener -> {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Notifying dataset change listener: {}", listener.getClass().getSimpleName());
          }

          listener.onDatasetChange(change);
        });
  }

  @Override
  public void handle(FeatureChange change) {
    featureListeners.forEach(listener -> listener.onFeatureChange(change));
  }
}
