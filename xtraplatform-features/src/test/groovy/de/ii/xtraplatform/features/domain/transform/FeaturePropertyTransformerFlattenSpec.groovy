/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform

import de.ii.xtraplatform.features.domain.FeatureTokenType
import de.ii.xtraplatform.features.domain.SchemaBase.Type
import spock.lang.Specification

class FeaturePropertyTransformerFlattenSpec extends Specification {

    static FeaturePropertyTransformerFlatten transformer() {
        return ImmutableFeaturePropertyTransformerFlatten.builder()
                .propertyPath("eeNachEnergietraeger")
                .parameter(".")
                .build()
    }

    static List<Object> object(List<String> path, List<Object>... nested) {
        def result = [FeatureTokenType.OBJECT, path]
        nested.each { result.addAll(it) }
        result.add(FeatureTokenType.OBJECT_END)
        result.add(path)
        return result
    }

    static List<Object> value(List<String> path, String val, Type type) {
        return [FeatureTokenType.VALUE, path, val, type]
    }

    // one array item, with a scalar directly on the item (energietraeger) and a nested
    // object on the item (endenergie, itself holding value/uom) - mirrors the
    // eeNachEnergietraeger / WP_EndenergieEnergietraeger / measure schema nesting
    static List<Object> arrayItem(String energietraeger, String endenergieValue, String endenergieUom) {
        return object(["eeNachEnergietraeger"],
                value(["eeNachEnergietraeger", "energietraeger"], energietraeger, Type.STRING),
                object(["eeNachEnergietraeger", "endenergie"],
                        value(["eeNachEnergietraeger", "endenergie", "value"], endenergieValue, Type.FLOAT),
                        value(["eeNachEnergietraeger", "endenergie", "uom"], endenergieUom, Type.STRING)))
    }

    static Map<String, String> flattenedValues(List<Object> transformed) {
        def result = [:]
        for (int i = 0; i < transformed.size(); i += 4) {
            List<String> path = transformed[i + 1] as List<String>
            result[path.join(".")] = transformed[i + 2] as String
        }
        return result
    }

    def 'a scalar directly on an array item is flattened with the item index'() {
        given: 'an array with one item that has a scalar property'
        def slice = [FeatureTokenType.ARRAY, ["eeNachEnergietraeger"]]
        slice.addAll(arrayItem("Erdgas", "123.4", "MWh"))
        slice.add(FeatureTokenType.ARRAY_END)
        slice.add(["eeNachEnergietraeger"])

        when: 'the slice is flattened'
        def transformed = transformer().transform("eeNachEnergietraeger", slice)

        then: 'the scalar keeps its own name next to the indexed array property'
        flattenedValues(transformed)["eeNachEnergietraeger[1].energietraeger"] == "Erdgas"
    }

    def 'an object nested two levels below an array item is flattened without dropping the intermediate name'() {
        given: 'an array item whose own property (endenergie) is itself an object'
        def slice = [FeatureTokenType.ARRAY, ["eeNachEnergietraeger"]]
        slice.addAll(arrayItem("Erdgas", "123.4", "MWh"))
        slice.add(FeatureTokenType.ARRAY_END)
        slice.add(["eeNachEnergietraeger"])

        when: 'the slice is flattened'
        def transformed = transformer().transform("eeNachEnergietraeger", slice)
        def values = flattenedValues(transformed)

        then: 'the intermediate object name (endenergie) is preserved, not dropped'
        values["eeNachEnergietraeger[1].endenergie.value"] == "123.4"
        values["eeNachEnergietraeger[1].endenergie.uom"] == "MWh"
    }

    def 'multiple array items are indexed independently'() {
        given: 'an array with two items, each with a two-level nested object'
        def slice = [FeatureTokenType.ARRAY, ["eeNachEnergietraeger"]]
        slice.addAll(arrayItem("Erdgas", "123.4", "MWh"))
        slice.addAll(arrayItem("Heizoel", "45.6", "MWh"))
        slice.add(FeatureTokenType.ARRAY_END)
        slice.add(["eeNachEnergietraeger"])

        when: 'the slice is flattened'
        def values = flattenedValues(transformer().transform("eeNachEnergietraeger", slice))

        then: 'each item keeps its own index for both the scalar and the nested object values'
        values["eeNachEnergietraeger[1].energietraeger"] == "Erdgas"
        values["eeNachEnergietraeger[1].endenergie.value"] == "123.4"
        values["eeNachEnergietraeger[2].energietraeger"] == "Heizoel"
        values["eeNachEnergietraeger[2].endenergie.value"] == "45.6"
    }
}
