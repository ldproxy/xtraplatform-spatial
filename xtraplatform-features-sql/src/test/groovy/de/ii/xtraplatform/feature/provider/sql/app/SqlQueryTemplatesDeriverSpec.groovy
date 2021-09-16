/*
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.provider.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.CqlFilter
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.feature.provider.sql.domain.SqlDialectPostGis
import de.ii.xtraplatform.features.domain.SortKey
import spock.lang.Shared
import spock.lang.Specification

class SqlQueryTemplatesDeriverSpec extends Specification {

    @Shared
    FilterEncoderSql filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPostGis(), null, new CqlImpl())
    @Shared
    SqlQueryTemplatesDeriver td = new SqlQueryTemplatesDeriver(filterEncoder, new SqlDialectPostGis(), true)
    @Shared
    SqlQueryTemplatesDeriver tdNoNm = new SqlQueryTemplatesDeriver(filterEncoder, new SqlDialectPostGis(), false)

    static Optional<CqlFilter> noFilter = Optional.empty()

    def 'meta query templates: #casename'() {

        when:

        SqlQueryTemplates templates = source.get(0).accept(deriver)
        String actual = meta(templates, sortBy, userFilter)

        then:

        actual == expected

        where:

        casename                      | deriver | sortBy                                                                            | userFilter | source                            || expected
        "basic"                       | td      | []                                                                                | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META
        "basic without numberMatched" | tdNoNm  | []                                                                                | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_WITHOUT_NUMBER_MATCHED
        "sortBy"                      | td      | [SortKey.of("created")]                                                           | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_SORT_BY
        "sortBy descending"           | td      | [SortKey.of("created", SortKey.Direction.DESCENDING)]                             | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_SORT_BY_DESC
        "sortBy mixed"                | td      | [SortKey.of("created", SortKey.Direction.DESCENDING), SortKey.of("lastModified")] | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_SORT_BY_MIXED
        "filter"                      | td      | []                                                                                | noFilter   | QuerySchemaFixtures.SIMPLE_FILTER || SqlQueryTemplatesFixtures.META_FILTER
    }

    def 'value query templates: #casename'() {

        when:

        SqlQueryTemplates templates = source.get(0).accept(deriver)
        List<String> actual = values(templates)

        then:

        actual == expected

        where:

        casename                    | deriver | source                                         || expected
        "value array"               | td      | QuerySchemaFixtures.VALUE_ARRAY                || SqlQueryTemplatesFixtures.VALUE_ARRAY
        "object array"              | td      | QuerySchemaFixtures.OBJECT_ARRAY               || SqlQueryTemplatesFixtures.OBJECT_ARRAY
        "merge"                     | td      | QuerySchemaFixtures.MERGE                      || SqlQueryTemplatesFixtures.MERGE
        "self joins"                | td      | QuerySchemaFixtures.SELF_JOINS                 || SqlQueryTemplatesFixtures.SELF_JOINS
        "self joins with filters"   | td      | QuerySchemaFixtures.SELF_JOINS_FILTER          || SqlQueryTemplatesFixtures.SELF_JOINS_FILTER
        "object without sourcePath" | td      | QuerySchemaFixtures.OBJECT_WITHOUT_SOURCE_PATH || SqlQueryTemplatesFixtures.OBJECT_WITHOUT_SOURCE_PATH
    }

    static String meta(SqlQueryTemplates templates, List<SortKey> sortBy, Optional<CqlFilter> userFilter) {
        return templates.getMetaQueryTemplate().generateMetaQuery(10, 10, sortBy, userFilter)
    }

    static List<String> values(SqlQueryTemplates templates) {
        return templates.getValueQueryTemplates().collect { it.generateValueQuery(10, 10, [], Optional.empty(), Optional.empty()) }
    }

}
