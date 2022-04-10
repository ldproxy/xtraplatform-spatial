package de.ii.xtraplatform.features.app;

import de.ii.xtraplatform.features.domain.FeatureChange;
import de.ii.xtraplatform.features.domain.FeatureChangeHandler;
import de.ii.xtraplatform.features.domain.FeatureChangeListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class FeatureChangeHandlerImpl implements FeatureChangeHandler {

  private final List<FeatureChangeListener> listeners;

  public FeatureChangeHandlerImpl() {
    this.listeners = new CopyOnWriteArrayList<>();
  }

  @Override
  public void addListener(FeatureChangeListener listener) {
    listeners.add(listener);
  }

  @Override
  public void handle(FeatureChange change) {
    listeners.forEach(listener -> listener.onFeatureChange(change));
  }
}
