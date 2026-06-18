/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.SchemaBase.Type
import spock.lang.Specification

// Regression for the FeatureEventBuffer schema-order reordering: a property backed by a joined
// table is produced by the provider after the main-table columns even when it is declared before
// them. The buffer must emit it at its schema position, not where its tokens first arrive —
// otherwise an object property declared before its scalar siblings ends up after them, producing
// XML/GML that does not match the application schema's element order.
class FeatureEventBufferOrderSpec extends Specification {

    // id, child (OBJECT on a joined table), a, b (scalar columns on the main table)
    static FeatureSchema SCHEMA = new ImmutableFeatureSchema.Builder()
            .name("t")
            .sourcePath("/t")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("objid").type(Type.STRING).role(SchemaBase.Role.ID))
            .putProperties2("child", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=rid]t__child")
                    .type(Type.OBJECT)
                    .putProperties2("value", new ImmutableFeatureSchema.Builder()
                            .sourcePath("value").type(Type.STRING)))
            .putProperties2("a", new ImmutableFeatureSchema.Builder()
                    .sourcePath("a").type(Type.STRING))
            .putProperties2("b", new ImmutableFeatureSchema.Builder()
                    .sourcePath("b").type(Type.STRING))
            .build()

    // provider order: the main-table scalars (a, b) arrive before the joined object (child)
    static List<Object> SOURCE = [
            FeatureTokenType.INPUT, true, FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE, ["t", "objid"], "f1", Type.STRING,
            FeatureTokenType.VALUE, ["t", "a"], "2000", Type.STRING,
            FeatureTokenType.VALUE, ["t", "b"], "1200", Type.STRING,
            FeatureTokenType.OBJECT, ["t", "[id=rid]t__child"],
            FeatureTokenType.VALUE, ["t", "[id=rid]t__child", "value"], "1000", Type.STRING,
            FeatureTokenType.OBJECT_END, ["t", "[id=rid]t__child"],
            FeatureTokenType.FEATURE_END, FeatureTokenType.INPUT_END
    ]

    // schema order: child is emitted before a/b, with its object markers and child value intact
    static List<Object> EXPECTED = [
            FeatureTokenType.INPUT, true, FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE, ["id"], "f1", Type.STRING,
            FeatureTokenType.OBJECT, ["child"],
            FeatureTokenType.VALUE, ["child", "value"], "1000", Type.STRING,
            FeatureTokenType.OBJECT_END, ["child"],
            FeatureTokenType.VALUE, ["a"], "2000", Type.STRING,
            FeatureTokenType.VALUE, ["b"], "1200", Type.STRING,
            FeatureTokenType.FEATURE_END, FeatureTokenType.INPUT_END
    ]

    def "a joined object declared before main-table columns is emitted at its schema position"() {
        given:
        List<Object> actual = []
        FeatureTokenReader reader = Util.createReader(SCHEMA, actual)

        when:
        SOURCE.forEach(token -> reader.onToken(token))

        then:
        actual == EXPECTED
    }
}
