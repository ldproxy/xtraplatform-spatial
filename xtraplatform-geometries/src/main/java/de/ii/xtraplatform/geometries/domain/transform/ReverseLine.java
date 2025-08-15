/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import de.ii.xtraplatform.geometries.domain.PositionList.Interpolation;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ReverseLine implements CoordinatesTransformation {

  /*
  lwpoly_force_clockwise(LWPOLY *poly)
  {
  	uint32_t i;

  	// No-op empties
  	if ( lwpoly_is_empty(poly) )
              return;

      // External ring
  	if ( ptarray_isccw(poly->rings[0]) )
      ptarray_reverse_in_place(poly->rings[0]);

      // Internal rings
  	for (i=1; i<poly->nrings; i++)
              if ( ! ptarray_isccw(poly->rings[i]) )
      ptarray_reverse_in_place(poly->rings[i]);

  }

  ptarray_isccw(const POINTARRAY *pa)
  {
  	double area = 0;
  	area = ptarray_signed_area(pa);
  	if ( area > 0 ) return LW_FALSE;
  	else return LW_TRUE;
  }

  ptarray_signed_area(const POINTARRAY *pa)
  {
  	const POINT2D *P1;
  	const POINT2D *P2;
  	const POINT2D *P3;
  	double sum = 0.0;
  	double x0, x, y1, y2;
  	uint32_t i;

  	if (! pa || pa->npoints < 3 )
  		return 0.0;

  	P1 = getPoint2d_cp(pa, 0);
  	P2 = getPoint2d_cp(pa, 1);
  	x0 = P1->x;
  	for ( i = 2; i < pa->npoints; i++ )
  	{
  		P3 = getPoint2d_cp(pa, i);
  		x = P2->x - x0;
  		y1 = P3->y;
  		y2 = P1->y;
  		sum += x * (y2-y1);

  		// Move forwards!
          P1 = P2;
          P2 = P3;
      }
  	return sum / 2.0;
  }

  SEE https://en.wikipedia.org/wiki/Curve_orientation and https://stackoverflow.com/questions/1165647/how-to-determine-if-a-list-of-polygon-points-are-in-clockwise-order/1180256#1180256
  for optimizations
       */

  @Override
  public double[] onCoordinates(
      double[] coordinates,
      int length,
      int dimension,
      Optional<Interpolation> interpolation,
      OptionalInt minNumberOfPositions)
      throws IOException {
    for (int i = 0; i < length / 2; i = i + dimension) {
      double x = coordinates[i];
      double y = coordinates[i + 1];

      int newXPos = length - 1 - i - (dimension - 1);
      int newYPos = newXPos + 1;

      coordinates[i] = coordinates[newXPos];
      coordinates[i + 1] = coordinates[newYPos];

      coordinates[newXPos] = x;
      coordinates[newYPos] = y;

      if (dimension >= 3) {
        double z = coordinates[i + 2];

        int newZPos = newYPos + 1;

        coordinates[i + 2] = coordinates[newZPos];

        coordinates[newZPos] = z;
      }

      if (dimension == 4) {
        double m = coordinates[i + 3];

        int newMPos = newYPos + 2;

        coordinates[i + 3] = coordinates[newMPos];

        coordinates[newMPos] = m;
      }
    }

    if (getNext().isEmpty()) {
      return coordinates;
    }

    return getNext()
        .get()
        .onCoordinates(coordinates, length, dimension, interpolation, minNumberOfPositions);
  }
}
