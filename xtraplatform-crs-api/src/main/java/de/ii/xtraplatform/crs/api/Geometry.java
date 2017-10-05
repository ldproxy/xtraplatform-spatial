/**
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
/**
 * bla
 */
package de.ii.xtraplatform.crs.api;

/**
 *
 * @author fischer
 */
public abstract class Geometry {

    protected EpsgCrs spatialReference;
    protected double[] coordinates;

    public double[] getCoords() {
        return coordinates;
    }

    public EpsgCrs getSpatialReference() {
        return spatialReference;
    }

    public void setSpatialReference(EpsgCrs spatialReference) {
        this.spatialReference = spatialReference;
    }
}
