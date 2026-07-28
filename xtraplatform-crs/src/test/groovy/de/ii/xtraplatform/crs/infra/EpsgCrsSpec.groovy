package de.ii.xtraplatform.crs.infra

import de.ii.xtraplatform.crs.domain.CoordinateTupleWithPrecision
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.crs.domain.OgcCrs
import spock.lang.Specification

class EpsgCrsSpec extends Specification {

    def 'fromString - simple'() {
        when:
        def crs = EpsgCrs.fromString("EPSG:25832")

        then:
        crs.code == 25832 && crs.forceAxisOrder == EpsgCrs.Force.NONE

    }

    def 'fromString - urn'() {
        when:
        def crs = EpsgCrs.fromString("urn:ogc:def:crs:EPSG::25832")

        then:
        crs.code == 25832

    }

    def 'fromString - uri'() {
        when:
        def crs = EpsgCrs.fromString("http://www.opengis.net/def/crs/EPSG/0/25832")

        then:
        crs.code == 25832

    }

    def 'fromString - crs84'() {
        when:
        def crs = EpsgCrs.fromString(OgcCrs.CRS84_URI)

        then:
        crs.code == 4326 && crs.forceAxisOrder == EpsgCrs.Force.LON_LAT

    }

    def 'fromString - crs84 alias'() {
        when:
        def crs = EpsgCrs.fromString(OgcCrs.CRS84_URI_NEW)

        then:
        crs.code == 4326
        crs.forceAxisOrder == EpsgCrs.Force.LON_LAT
        crs == OgcCrs.CRS84
        crs.hashCode() == OgcCrs.CRS84.hashCode()
        crs.toUriString() == OgcCrs.CRS84_URI_NEW
        crs.toUrnString() == "urn:ogc:def:crs:OGC::4326"
        crs.toSafeCurie() == OgcCrs.CRS84_CURIE

    }

    def 'fromString - crs84h'() {
        when:
        def crs = EpsgCrs.fromString(OgcCrs.CRS84h_URI)

        then:
        crs.code == 4979 && crs.forceAxisOrder == EpsgCrs.Force.LON_LAT

    }

    def 'fromString - vertical'() {
        when:
        def crs = EpsgCrs.fromString("EPSG:25832", "EPSG:7837")

        then:
        crs.code == 25832 && crs.verticalCode.isPresent() && crs.verticalCode.getAsInt() == 7837

    }

    def 'fromString - exception'() {
        when:
        def crs = EpsgCrs.fromString("EPSG:258xxx32")

        then:
        thrown(IllegalArgumentException)

    }

    def 'toString - simple'() {
        when:
        def crs = EpsgCrs.of(25832, 7837, EpsgCrs.Force.LAT_LON)

        then:
        crs.toSimpleString() == "EPSG:25832"

    }

    def 'toString - urn'() {
        when:
        def crs = EpsgCrs.of(25832)

        then:
        crs.toUrnString() == "urn:ogc:def:crs:EPSG::25832"

    }

    def 'toString - uri'() {
        expect:
        crs.toUriString() == uri

        where:
        crs                | uri
        EpsgCrs.of(25832)  | "http://www.opengis.net/def/crs/EPSG/0/25832"
        OgcCrs.CRS84       | OgcCrs.CRS84_URI
        OgcCrs.CRS84_OGC0  | OgcCrs.CRS84_URI_NEW
        OgcCrs.CRS84h      | OgcCrs.CRS84h_URI
    }

    def 'allUris'() {
        expect:
        crs.allUris() == uris

        where:
        crs                | uris
        EpsgCrs.of(25832)  | ["http://www.opengis.net/def/crs/EPSG/0/25832"]
        OgcCrs.CRS84       | [OgcCrs.CRS84_URI, OgcCrs.CRS84_URI_NEW]
        OgcCrs.CRS84_OGC0  | [OgcCrs.CRS84_URI, OgcCrs.CRS84_URI_NEW]
        OgcCrs.CRS84h      | [OgcCrs.CRS84h_URI]
    }

    def 'CoordinateTupleWithPrecision - uri'() {
        expect:
        coords.getXasString() == x && coords.getYasString() == y

        where:
        coords                                                           | x      | y
        new CoordinateTupleWithPrecision(new double[]{50.5, 7.5}, false) | "50.5" | "7.5"
        new CoordinateTupleWithPrecision(new double[]{50.5, 7.5}, true)  | "50.5" | "7.5"
    }
}
