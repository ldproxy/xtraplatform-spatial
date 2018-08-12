/**
 * Copyright 2018 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
/**
 * bla
 */
package de.ii.xtraplatform.crs.api;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *
 * @author zahnen
 */
public class EpsgCrs {

    private static final String SIMPLE_PREFIX = "EPSG:";
    private static final String URN_PREFIX = "urn:ogc:def:crs:EPSG::";
    private static final String URI_PREFIX = "http://www.opengis.net/def/crs/EPSG/0/";
    private int code;
    private boolean forceLongitudeFirst = false;

    public EpsgCrs() {
        this.code = -1;
        this.forceLongitudeFirst = false;
    }

    public EpsgCrs(int code) {
        this();
        this.code = code;
    }

    public EpsgCrs(int code, boolean forceLongitudeFirst) {
        this(code);
        this.forceLongitudeFirst = forceLongitudeFirst;
    }

    public EpsgCrs(String prefixedCode) {
        this(parsePrefixedCode(prefixedCode));
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public boolean isForceLongitudeFirst() {
        return forceLongitudeFirst;
    }

    public void setForceLongiduteFirst(boolean forceLongitudeFirst) {
        this.forceLongitudeFirst = forceLongitudeFirst;
    }

    @JsonIgnore
    public String getAsSimple() {
        return SIMPLE_PREFIX.concat(Integer.toString(code));
    }

    @JsonIgnore
    public String getAsUrn() {
        return URN_PREFIX.concat(Integer.toString(code));
    }

    @JsonIgnore
    public String getAsUri() {
        return URI_PREFIX.concat(Integer.toString(code));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final EpsgCrs other = (EpsgCrs) obj;
        if (code != other.getCode() || forceLongitudeFirst != other.isForceLongitudeFirst())
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = code;
        result = 31 * result + (forceLongitudeFirst ? 1 : 0);
        return result;
    }

    private static int parsePrefixedCode(String prefixedCode) {
        int code = -1;

        try {
            code = Integer.valueOf(prefixedCode.substring(prefixedCode.lastIndexOf(":") + 1));
        } catch (NumberFormatException e) {
            try {
                code = Integer.valueOf(prefixedCode.substring(prefixedCode.lastIndexOf("/") + 1));
            } catch (NumberFormatException e2) {
                try {
                    code = Integer.valueOf(prefixedCode);
                } catch (NumberFormatException e3) {
                    // ignore
                }
            }
        }

        return code;
    }
}
