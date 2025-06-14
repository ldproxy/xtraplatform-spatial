/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import de.ii.xtraplatform.crs.domain.BoundingBox
import de.ii.xtraplatform.crs.domain.EpsgCrs
import org.threeten.extra.Interval
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.time.DateTimeException
import java.time.ZoneId

class SqlDialectPostGisSpec extends Specification {

    @Shared SqlDialectPgis sqlDialectPostGis

    def setupSpec() {
        sqlDialectPostGis = new SqlDialectPgis()
    }

    def 'Temporal extent start and end are present and correctly formatted'() {
        given:
            String start = "2008-05-21 13:21:48"
            String end = "2015-12-21 22:17:56"
        when:
            Optional<Interval> interval = sqlDialectPostGis.parseTemporalExtent(start, end, ZoneId.of("UTC"))
        then:
            interval.get().toString() == "2008-05-21T13:21:48Z/2015-12-21T22:17:56Z"
    }

    def 'Temporal extent start is null'() {
        given:
            String end = "2015-12-21 22:17:56"
        when:
            Optional<Interval> interval = sqlDialectPostGis.parseTemporalExtent(null, end, ZoneId.of("UTC"))
        then:
            interval.isEmpty()
    }

    def 'Temporal extent end is null'() {
        given:
            String start = "2008-05-21 13:21:48"
        when:
            Optional<Interval> interval = sqlDialectPostGis.parseTemporalExtent(start, null, ZoneId.of("UTC"))
        then:
            interval.get().isUnboundedEnd()
    }

    def 'Temporal extent start and end are null'() {
        when:
            Optional<Interval> interval = sqlDialectPostGis.parseTemporalExtent(null, null, ZoneId.of("UTC"))
        then:
            interval.isEmpty()
    }

    def 'Temporal extent start > end'() {
        given:
            String end = "2008-05-21 13:21:48"
            String start = "2015-12-21 22:17:56"
        when:
            Optional<Interval> interval = sqlDialectPostGis.parseTemporalExtent(start, end, ZoneId.of("UTC"))
        then:
            thrown(DateTimeException)
    }

    def 'Temporal extent start or end unexpected format'() {
        given:
            String end = "2008-05-21"
            String start = "2015-12-21"
        when:
            Optional<Interval> interval = sqlDialectPostGis.parseTemporalExtent(start, end, ZoneId.of("UTC"))
        then:
            thrown(DateTimeException)
    }

    def 'Spatial extent and CRS both present'() {
        given:
            String bbox = "BOX(2.336059,50.664734,7.304131,55.433815)"
            EpsgCrs crs = EpsgCrs.of(4326)
        when:
            Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(bbox, crs)
        then:
            extent.get().toArray().toString() == "[2.336059, 50.664734, 7.304131, 55.433815]"
            extent.get().getEpsgCrs().getCode() == 4326
    }

    def 'Spatial extent and CRS both present 2'() {
        given:
        String bbox = "BOX(2.336059,50.664734,7.304131,55.433815, 50.664734,7.304131 )"
        EpsgCrs crs = EpsgCrs.of(4326)
        when:
        Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(bbox, crs)
        then:
        extent.get().toArray().toString() != "[2.336059, 50.664734, 7.304131, 55.433815]"
        extent.get().getEpsgCrs().getCode() == 4326
    }

    def 'Spatial extent and CRS both present 3'() {
        given:
        String bbox = null
        EpsgCrs crs = EpsgCrs.of(4326)
        when:
        Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(bbox, crs)
        then:
        extent.isPresent() == false
    }

    def 'Spatial extent and CRS are null'() {
        when:
            Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(null, null)
        then:
            extent.isEmpty()
    }

    def 'Spatial extent is present, CRS is null'() {
        given:
            String bbox = "BOX(2.336059,50.664734,7.304131,55.433815)"
        when:
            Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(bbox, null)
        then:
            extent.isEmpty()
    }

    def 'Spatial extent is null, CRS is present'() {
        given:
            EpsgCrs crs = EpsgCrs.of(4326)
        when:
            Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(null, crs)
        then:
            extent.isEmpty()
    }

    def 'Spatial extent with incomplete coordinates'() {
        given:
            String bbox = "BOX(2.336059,50.664734,7.304131)"
            EpsgCrs crs = EpsgCrs.of(4326)
        when:
            Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(bbox, crs)
        then:
            extent.isEmpty()
    }

    def 'Spatial extent in unexpected format'() {
        given:
            String bbox = "2.336059,50.664734,7.304131,55.433815"
            EpsgCrs crs = EpsgCrs.of(4326)
        when:
            Optional<BoundingBox> extent = sqlDialectPostGis.parseExtent(bbox, crs)
        then:
            extent.isEmpty()
    }

}
