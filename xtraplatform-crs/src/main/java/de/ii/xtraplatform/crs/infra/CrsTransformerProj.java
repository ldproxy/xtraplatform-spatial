/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.infra;

import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.crs.domain.BoundingBoxTransformer;
import de.ii.xtraplatform.crs.domain.CoordinateTuple;
import de.ii.xtraplatform.crs.domain.CoordinateTupleWithPrecision;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.Optional;
import javax.measure.Unit;
import org.kortforsyningen.proj.Units;
import org.opengis.referencing.crs.CompoundCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.SingleCRS;
import org.opengis.referencing.datum.GeodeticDatum;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
public class CrsTransformerProj extends BoundingBoxTransformer implements CrsTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrsTransformerProj.class);

  private final EpsgCrs sourceCrs;
  private final EpsgCrs targetCrs;
  private final boolean isSourceMetric;
  private final boolean isTargetMetric;
  private final double sourceUnitEquivalentInMeters;
  private final double targetUnitEquivalentInMeters;
  private final int sourceDimension;
  private final int targetDimension;
  private final CoordinateOperation operation;
  private final Optional<CoordinateOperation> horizontalOperation;

  CrsTransformerProj(
      CoordinateReferenceSystem sourceCrs,
      CoordinateReferenceSystem targetCrs,
      EpsgCrs origSourceCrs,
      EpsgCrs origTargetCrs,
      int sourceDimension,
      int targetDimension,
      CoordinateOperation coordinateOperation,
      Optional<CoordinateOperation> horizontalCoordinateOperation) {
    super();
    this.sourceCrs = origSourceCrs;
    this.targetCrs = origTargetCrs;

    Unit<?> sourceUnit = sourceCrs.getCoordinateSystem().getAxis(0).getUnit();
    Unit<?> targetUnit = targetCrs.getCoordinateSystem().getAxis(0).getUnit();

    this.isSourceMetric = Units.METRE.equals(sourceUnit);
    this.isTargetMetric = Units.METRE.equals(targetUnit);

    SingleCRS horizontalSourceCrs = getHorizontalCrs(sourceCrs);
    SingleCRS horizontalTargetCrs = getHorizontalCrs(targetCrs);

    this.sourceUnitEquivalentInMeters = getUnitEquivalentInMeters(horizontalSourceCrs, true);
    this.targetUnitEquivalentInMeters = getUnitEquivalentInMeters(horizontalTargetCrs, false);

    this.sourceDimension = sourceDimension;
    this.targetDimension = targetDimension;

    this.operation = coordinateOperation;
    this.horizontalOperation = horizontalCoordinateOperation;
  }

  @Override
  public EpsgCrs getSourceCrs() {
    return sourceCrs;
  }

  @Override
  public EpsgCrs getTargetCrs() {
    return targetCrs;
  }

  @Override
  public CoordinateTuple transform(double x, double y) {
    return transform(new CoordinateTuple(x, y));
  }

  @Override
  public CoordinateTuple transform(CoordinateTuple coordinateTuple) {
    return new CoordinateTupleWithPrecision(
        transform(coordinateTuple.asArray(), 1, 2), isTargetMetric);
  }

  @Override
  @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
  public double[] transform(double[] coordinates, int numberOfPoints, int dimension) {
    if (dimension > sourceDimension) {
      throw new IllegalStateException(
          String.format(
              "Mismatched coordinate dimension for a CRS transformation, expected '%d' but got '%d'.",
              sourceDimension, dimension));
    }

    double[] target = new double[dimension * numberOfPoints];

    try {
      getMathTransform(dimension).transform(coordinates, 0, target, 0, numberOfPoints);

      return target;
    } catch (Throwable ex) {
      LogContext.errorAsDebug(LOGGER, ex, "PROJ");
    }

    return null;
  }

  @Override
  public double getSourceUnitEquivalentInMeters() {
    return sourceUnitEquivalentInMeters;
  }

  @Override
  public double getTargetUnitEquivalentInMeters() {
    return targetUnitEquivalentInMeters;
  }

  @Override
  public int getSourceDimension() {
    return sourceDimension;
  }

  @Override
  public int getTargetDimension() {
    return targetDimension;
  }

  private MathTransform getMathTransform(int dimension) {
    CoordinateOperation coordinateOperation =
        dimension == 2 ? horizontalOperation.orElse(operation) : operation;

    return coordinateOperation.getMathTransform();
  }

  private double getUnitEquivalentInMeters(SingleCRS horizontalCrs, boolean isSource) {
    return (isSource ? isSourceMetric : isTargetMetric)
            || !(horizontalCrs.getDatum() instanceof GeodeticDatum)
        ? 1
        : (Math.PI / 180.00)
            * ((GeodeticDatum) horizontalCrs.getDatum()).getEllipsoid().getSemiMajorAxis();
  }

  private SingleCRS getHorizontalCrs(CoordinateReferenceSystem crs) {
    return crs instanceof CompoundCRS
        ? (SingleCRS) ((CompoundCRS) crs).getComponents().get(0)
        : (SingleCRS) crs;
  }
}
