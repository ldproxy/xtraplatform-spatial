/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.geometries.domain.Point
import spock.lang.Specification

import javax.xml.stream.XMLOutputFactory

class GeometrySrsNameMapperSpec extends Specification {

    def 'TEMPLATE mapper rewrites srsName for matching CRS'() {
        given:
        EpsgCrs etrs89Utm32 = EpsgCrs.of(25832)
        Geometry<?> geometry = Point.of(389000.0, 5705000.0).withCrs(etrs89Utm32)
        def sw = new StringWriter()
        def xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw)
        def encoder = new GeometryEncoderGml(
                xmlWriter,
                GmlVersion.GML32,
                Set.of(GeometryEncoderGml.Options.WITH_SRS_NAME),
                Optional.of("gml"),
                Optional.empty(),
                List.of(),
                { EpsgCrs crs -> etrs89Utm32 == crs ? 'urn:adv:crs:ETRS89_UTM32' : crs.toUriString() } as java.util.function.Function)

        when:
        geometry.accept(encoder)
        xmlWriter.flush()
        String gmlOut = sw.toString()

        then:
        gmlOut.contains('srsName="urn:adv:crs:ETRS89_UTM32"')
    }

    def 'TEMPLATE mapper falls back to OGC URI for unmapped CRS'() {
        given:
        EpsgCrs etrs89Utm32 = EpsgCrs.of(25832)
        EpsgCrs wgs84Utm32n = EpsgCrs.of(32632)
        Geometry<?> geometry = Point.of(389000.0, 5705000.0).withCrs(wgs84Utm32n)
        def sw = new StringWriter()
        def xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw)
        def encoder = new GeometryEncoderGml(
                xmlWriter,
                GmlVersion.GML32,
                Set.of(GeometryEncoderGml.Options.WITH_SRS_NAME),
                Optional.of("gml"),
                Optional.empty(),
                List.of(),
                { EpsgCrs crs -> etrs89Utm32 == crs ? 'urn:adv:crs:ETRS89_UTM32' : crs.toUriString() } as java.util.function.Function)

        when:
        geometry.accept(encoder)
        xmlWriter.flush()
        String gmlOut = sw.toString()

        then:
        gmlOut.contains('srsName="http://www.opengis.net/def/crs/EPSG/0/32632"')
    }

    def 'OGC default constructor preserves toUriString behavior'() {
        given:
        Geometry<?> geometry = Point.of(389000.0, 5705000.0).withCrs(EpsgCrs.of(25832))
        def sw = new StringWriter()
        def xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw)
        def encoder = new GeometryEncoderGml(
                xmlWriter,
                GmlVersion.GML32,
                Set.of(GeometryEncoderGml.Options.WITH_SRS_NAME),
                Optional.of("gml"),
                Optional.empty(),
                List.of())

        when:
        geometry.accept(encoder)
        xmlWriter.flush()
        String gmlOut = sw.toString()

        then:
        gmlOut.contains('srsName="http://www.opengis.net/def/crs/EPSG/0/25832"')
    }
}
