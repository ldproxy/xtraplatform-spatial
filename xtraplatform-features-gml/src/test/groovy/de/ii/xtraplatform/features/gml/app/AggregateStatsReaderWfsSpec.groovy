/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app

import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.SchemaBase
import spock.lang.Specification

class AggregateStatsReaderWfsSpec extends Specification {

    def "the bounding box of a prefixed feature type is found under the ids of the capabilities analyzer"() {

        given: 'a type whose source path is the prefixed name of the WFS feature type'
        def schema = new ImmutableFeatureSchema.Builder()
                .name("rbz")
                .sourcePath("/dvg:nw_DVG1_rbz")
                .type(SchemaBase.Type.OBJECT)
                .build()

        expect: 'the type id, then prefix_localname and localname in lower case, not the prefixed name'
        AggregateStatsReaderWfs.getTypeIds(schema) == ["rbz", "dvg_nw_dvg1_rbz", "nw_dvg1_rbz"]
    }

    def "a feature type without a prefix"() {

        given:
        def schema = new ImmutableFeatureSchema.Builder()
                .name("flurstueck")
                .sourcePath("/Flurstueck")
                .type(SchemaBase.Type.OBJECT)
                .build()

        expect:
        AggregateStatsReaderWfs.getTypeIds(schema) == ["flurstueck", "flurstueck"]
    }
}
