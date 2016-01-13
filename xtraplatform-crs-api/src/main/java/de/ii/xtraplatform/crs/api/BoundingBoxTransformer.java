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
package de.ii.xtraplatform.crs.api;

/**
 *
 * @author fischer
 */
public abstract class BoundingBoxTransformer implements CrsTransformer{
        
    @Override
    public BoundingBox transformBoundingBox(BoundingBox boundingBox, EpsgCrs targetCrs) throws CrsTransformationException {
                
        // DEBUG
        //System.out.println( bbox.getXmin()+" " +bbox.getYmin()+" "+bbox.getXmax()+" "+bbox.getYmax()+" 0;");
        CoordinateTuple ll = this.transform(boundingBox.getXmin(), boundingBox.getYmin());
        CoordinateTuple lr = this.transform(boundingBox.getXmax(), boundingBox.getYmin());
        CoordinateTuple ur = this.transform(boundingBox.getXmax(), boundingBox.getYmax());
        CoordinateTuple ul = this.transform(boundingBox.getXmin(), boundingBox.getYmax());

        if (ll.isNull() || ul.isNull() || lr.isNull() || ur.isNull()) {
            throw new CrsTransformationException();
        }

        BoundingBox envOut = new BoundingBox();
        envOut.setEpsgCrs(targetCrs);
              
        // Die BoundingBox ins boxIn (lx,ly,hx,hy) im System crsIn wird in das System 
        // crsOut transformiert. Dabei wird sichergestellt, dass es sich wieder um eine
        // BoundingBox handelt. D.h. in Wirklichkeit werden alle vier Eckpunkte der
        // Input-BoundingBox transformiert und davon wird die Output-BoundingBox durch
        // Minmaxing bestimmt.
        
        if (ul.getX() < ll.getX()) {
            envOut.setXmin(ul.getX());
        } else {
            envOut.setXmin(ll.getX());
        }
        if (lr.getY() < ll.getY()) {
            envOut.setYmin(lr.getY());
        } else {
            envOut.setYmin(ll.getY());
        }
        if (ur.getX() > lr.getX()) {
            envOut.setXmax(ur.getX());
        } else {
            envOut.setXmax(lr.getX());
        }
        if (ul.getY() > ur.getY()) {
            envOut.setYmax(ul.getY());
        } else {
            envOut.setYmax(ur.getY());
        }

        // DEBUG
        //System.out.println( envOut.getXmin()+" " +envOut.getYmin()+" "+envOut.getXmax()+" "+envOut.getYmax()+" 1;");
        return envOut;        
    }
}
