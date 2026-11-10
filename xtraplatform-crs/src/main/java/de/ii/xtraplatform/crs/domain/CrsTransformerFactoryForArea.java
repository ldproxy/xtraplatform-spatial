/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry.ChangeHandler;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import javax.measure.Unit;
import org.opengis.referencing.cs.AxisDirection;

/**
 * A view of a {@link CrsTransformerFactory} that selects every transformer for a fixed area of
 * interest. The view is not registered as a volatile itself, the delegate is.
 */
class CrsTransformerFactoryForArea implements CrsTransformerFactory {

  private final CrsTransformerFactory delegate;
  private final Optional<BoundingBox> areaOfInterest;

  CrsTransformerFactoryForArea(CrsTransformerFactory delegate, BoundingBox areaOfInterest) {
    this.delegate = Objects.requireNonNull(delegate);
    this.areaOfInterest = Optional.of(areaOfInterest);
  }

  static CrsTransformerFactory of(CrsTransformerFactory delegate, BoundingBox areaOfInterest) {
    return delegate instanceof CrsInfo
        ? new WithCrsInfo(delegate, areaOfInterest)
        : new CrsTransformerFactoryForArea(delegate, areaOfInterest);
  }

  @Override
  public boolean isSupported(EpsgCrs crs) {
    return delegate.isSupported(crs);
  }

  @Override
  public Optional<CrsTransformer> getTransformer(EpsgCrs sourceCrs, EpsgCrs targetCrs) {
    return delegate.getTransformer(sourceCrs, targetCrs, false, areaOfInterest);
  }

  @Override
  public Optional<CrsTransformer> getTransformer(
      EpsgCrs sourceCrs, EpsgCrs targetCrs, boolean force2d) {
    return delegate.getTransformer(sourceCrs, targetCrs, force2d, areaOfInterest);
  }

  /** An explicit area of interest takes precedence over the area of this view. */
  @Override
  public Optional<CrsTransformer> getTransformer(
      EpsgCrs sourceCrs, EpsgCrs targetCrs, boolean force2d, Optional<BoundingBox> areaOfInterest) {
    return delegate.getTransformer(
        sourceCrs, targetCrs, force2d, areaOfInterest.or(() -> this.areaOfInterest));
  }

  @Override
  public CrsTransformerFactory withAreaOfInterest(BoundingBox areaOfInterest) {
    return of(delegate, areaOfInterest);
  }

  @Override
  public String getUniqueKey() {
    return delegate.getUniqueKey();
  }

  @Override
  public Optional<String> getInstanceId() {
    return delegate.getInstanceId();
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public Optional<String> getMessage() {
    return delegate.getMessage();
  }

  @Override
  public Runnable onStateChange(ChangeHandler handler, boolean initialCall) {
    return delegate.onStateChange(handler, initialCall);
  }

  /** The view for a delegate that also provides {@link CrsInfo}. */
  static final class WithCrsInfo extends CrsTransformerFactoryForArea implements CrsInfo {

    private final CrsInfo crsInfo;

    WithCrsInfo(CrsTransformerFactory delegate, BoundingBox areaOfInterest) {
      super(delegate, areaOfInterest);
      this.crsInfo = (CrsInfo) delegate;
    }

    @Override
    public boolean is3d(EpsgCrs crs) {
      return crsInfo.is3d(crs);
    }

    @Override
    public Unit<?> getUnit(EpsgCrs crs) {
      return crsInfo.getUnit(crs);
    }

    @Override
    public List<String> getAxisAbbreviations(EpsgCrs crs) {
      return crsInfo.getAxisAbbreviations(crs);
    }

    @Override
    public List<Unit<?>> getAxisUnits(EpsgCrs crs) {
      return crsInfo.getAxisUnits(crs);
    }

    @Override
    public List<Optional<Double>> getAxisMinimums(EpsgCrs crs) {
      return crsInfo.getAxisMinimums(crs);
    }

    @Override
    public List<Optional<Double>> getAxisMaximums(EpsgCrs crs) {
      return crsInfo.getAxisMaximums(crs);
    }

    @Override
    public OptionalInt getAxisWithWraparound(EpsgCrs crs) {
      return crsInfo.getAxisWithWraparound(crs);
    }

    @Override
    public List<AxisDirection> getAxisDirections(EpsgCrs crs) {
      return crsInfo.getAxisDirections(crs);
    }

    @Override
    public Optional<BoundingBox> getDomainOfValidity(EpsgCrs crs) {
      return crsInfo.getDomainOfValidity(crs);
    }

    @Override
    public List<Integer> getPrecisionList(EpsgCrs crs, Map<String, Integer> coordinatePrecision) {
      return crsInfo.getPrecisionList(crs, coordinatePrecision);
    }
  }
}
