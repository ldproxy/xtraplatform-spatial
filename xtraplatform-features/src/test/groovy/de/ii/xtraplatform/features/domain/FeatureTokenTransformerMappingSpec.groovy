/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain


import spock.lang.Specification

/**
 * @author zahnen
 */
class FeatureTokenTransformerMappingSpec extends Specification {


    def 'feature mapping: #casename'() {

        given:

        List<Object> actual = []
        FeatureTokenReader reader = Util.createReader(schema, actual)

        when:

        source.forEach(token -> reader.onToken(token))

        then:

        actual == expected

        where:

        casename                                  | schema                                           | source                                                 | expected
        "joined value array between main columns" | FeatureSchemaFixtures.BIOTOP                     | FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY_AT_END | FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY_IN_ORDER_MAPPED
        "joined object array"                     | FeatureSchemaFixtures.OBJECT_ARRAY               | FeatureTokenFixtures.EXPLORATION_SITE_OBJECT_ARRAY     | FeatureTokenFixtures.EXPLORATION_SITE_OBJECT_ARRAY_MAPPED
        //"object without source path"              | FeatureSchemaFixtures.OBJECT_WITHOUT_SOURCE_PATH | FeatureTokenFixtures.OBJECT_WITHOUT_SOURCE_PATH        | FeatureTokenFixtures.OBJECT_WITHOUT_SOURCE_PATH_MAPPED

    }

}
