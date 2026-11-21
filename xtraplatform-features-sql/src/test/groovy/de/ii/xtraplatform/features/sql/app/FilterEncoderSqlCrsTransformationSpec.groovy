/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlFilterExamples
import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.crs.domain.CrsTransformer
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import spock.lang.Specification

/**
 * A filter geometry whose coordinates cannot be transformed to the native CRS, e.g. because they
 * are outside the valid range of the filter CRS, is an invalid filter (400), not a server error.
 */
class FilterEncoderSqlCrsTransformationSpec extends Specification {

    def 'coordinates that cannot be transformed make the filter invalid'() {
        given: 'a transformer that cannot transform the coordinates'
        CrsTransformer transformer = Stub()
        transformer.transform(_ as double[], _ as int, _ as int) >> null
        transformer.getSourceCrs() >> OgcCrs.CRS84
        transformer.getTargetCrs() >> OgcCrs.CRS84
        CrsTransformerFactory crsTransformerFactory = Stub()
        crsTransformerFactory.getTransformer(*_) >> Optional.of(transformer)
        def filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), crsTransformerFactory, null, new CqlImpl(), null)

        and: 'a spatial filter with a geometry in another CRS than the native CRS'
        def filter = CqlFilterExamples.EXAMPLE_15_RandomCrs

        when:
        filterEncoder.encode(filter, QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.startsWith('Filter is invalid. The coordinates of a geometry cannot be transformed from the filter CRS')
    }
}
