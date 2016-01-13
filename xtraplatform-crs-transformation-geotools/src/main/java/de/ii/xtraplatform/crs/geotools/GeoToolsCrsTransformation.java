/**
 * Copyright 2016 interactive instruments GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ii.xtraplatform.crs.geotools;

import de.ii.xsf.logging.XSFLogger;
import de.ii.xtraplatform.crs.api.CrsTransformation;
import de.ii.xtraplatform.crs.api.CrsTransformer;
import de.ii.xtraplatform.crs.api.EpsgCrs;
import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.forgerock.i18n.slf4j.LocalizedLogger;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

/**
 *
 * @author zahnen
 */
@Component
@Provides
@Instantiate
public class GeoToolsCrsTransformation implements CrsTransformation {

    private static final LocalizedLogger LOGGER = XSFLogger.getLogger(GeoToolsCrsTransformation.class);

    @Override
    public boolean isCrsSupported(String crs) {

        try {
            CRS.decode(applyWorkarounds(crs));
        } catch (FactoryException ex) {
            return false;
        }

        return true;
    }

    @Override
    public boolean isCrsAxisOrderEastNorth(String crs) {
        try {      
            return CRS.getAxisOrder(CRS.decode(applyWorkarounds(crs))) == CRS.AxisOrder.EAST_NORTH;
        } catch (FactoryException ex) {
            // ignore
        }

        return false;
    }

    @Override
    public CrsTransformer getTransformer(String sourceCrs, String targetCrs) {
        try {
            return new GeoToolsCrsTransformer(CRS.decode(applyWorkarounds(sourceCrs)), CRS.decode(applyWorkarounds(targetCrs)));
        } catch (FactoryException ex) {
            LOGGER.getLogger().debug("GeoTools error", ex);
        }
        return null;
    }

    @Override
    public CrsTransformer getTransformer(EpsgCrs sourceCrs, EpsgCrs targetCrs) {
        try {
            return new GeoToolsCrsTransformer(CRS.decode(applyWorkarounds(sourceCrs.getAsSimple()), sourceCrs.isLongitudeFirst()), CRS.decode(applyWorkarounds(targetCrs.getAsSimple()), targetCrs.isLongitudeFirst()));
        } catch (FactoryException ex) {
            LOGGER.getLogger().debug("GeoTools error", ex);
        }
        return null;
    }

    private String applyWorkarounds(String code) {
        // ArcGIS still uses code 102100, but GeoTools does not support it anymore
        if (code.endsWith("102100")) {
            return code.replace("102100", "3857");
        }
        return code;
    }

    @Override
    public double getUnitEquivalentInMeter(String crs) {
        try {
            Unit unit = CRS.getHorizontalCRS(CRS.decode(applyWorkarounds(crs))).getCoordinateSystem().getAxis(0).getUnit();

            if( unit != SI.METER) {
                return (Math.PI/180.00) * CRS.getEllipsoid(CRS.decode(applyWorkarounds(crs))).getSemiMajorAxis();
            }
        } catch (FactoryException ex) {
            LOGGER.getLogger().debug("GeoTools error", ex);
        }

        return 1;
    }
}
