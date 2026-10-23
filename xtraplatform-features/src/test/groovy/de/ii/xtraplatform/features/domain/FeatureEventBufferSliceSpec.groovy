/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.SchemaBase.Type
import de.ii.xtraplatform.features.domain.transform.FeatureEventBuffer
import spock.lang.Specification

// A property produced from more than one table arrives as several fragments. When an unrelated
// property is produced between those fragments (provider order), the slice handed to an in-buffer
// transformer (e.g. concat) must still contain only the property's own tokens - otherwise the
// transformer would swallow the unrelated property into it (e.g. nest a separate array inside a
// concatenated object array). The buffer guarantees this by ordering itself before a slice is read.
class FeatureEventBufferSliceSpec extends Specification {

    // arrA and arrB are both top-level object arrays; arrA is declared first
    static FeatureSchema SCHEMA = new ImmutableFeatureSchema.Builder()
            .name("t").sourcePath("/t").type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder().sourcePath("objid").type(Type.STRING).role(SchemaBase.Role.ID))
            .putProperties2("arrA", new ImmutableFeatureSchema.Builder().sourcePath("[id=a]t__arrA").type(Type.OBJECT_ARRAY)
                    .putProperties2("x", new ImmutableFeatureSchema.Builder().sourcePath("x").type(Type.STRING)))
            .putProperties2("arrB", new ImmutableFeatureSchema.Builder().sourcePath("[id=b]t__arrB").type(Type.OBJECT_ARRAY)
                    .putProperties2("y", new ImmutableFeatureSchema.Builder().sourcePath("y").type(Type.STRING)))
            .build()

    // provider order: arrA fragment 1, then arrB, then arrA fragment 2 (arrA split around arrB)
    static List<Object> SOURCE = [
            FeatureTokenType.VALUE, ["id"], "f1", Type.STRING,
            FeatureTokenType.ARRAY, ["arrA"],
            FeatureTokenType.OBJECT, ["arrA"], FeatureTokenType.VALUE, ["arrA", "x"], "1", Type.STRING, FeatureTokenType.OBJECT_END, ["arrA"],
            FeatureTokenType.ARRAY_END, ["arrA"],
            FeatureTokenType.ARRAY, ["arrB"],
            FeatureTokenType.OBJECT, ["arrB"], FeatureTokenType.VALUE, ["arrB", "y"], "b", Type.STRING, FeatureTokenType.OBJECT_END, ["arrB"],
            FeatureTokenType.ARRAY_END, ["arrB"],
            FeatureTokenType.ARRAY, ["arrA"],
            FeatureTokenType.OBJECT, ["arrA"], FeatureTokenType.VALUE, ["arrA", "x"], "2", Type.STRING, FeatureTokenType.OBJECT_END, ["arrA"],
            FeatureTokenType.ARRAY_END, ["arrA"]
    ]

    // a schema with a present property followed by an absent one. Shrinking the present property's
    // slice shifts every later position by the (negative) size delta; an absent position left at
    // start 0 would be driven negative and a subsequent getSlice would throw fromIndex < 0.
    static FeatureSchema SCHEMA_ABSENT = new ImmutableFeatureSchema.Builder()
            .name("t").sourcePath("/t").type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder().sourcePath("objid").type(Type.STRING).role(SchemaBase.Role.ID))
            .putProperties2("a", new ImmutableFeatureSchema.Builder().sourcePath("a").type(Type.STRING))
            .putProperties2("b", new ImmutableFeatureSchema.Builder().sourcePath("b").type(Type.STRING))
            .build()

    def "getSlice on an absent position after a shrunk slice does not go negative"() {
        given:
        FeatureEventBuffer buffer = Util.createBuffer(SCHEMA_ABSENT, [])
        buffer.reset("test")
        // id and a are present, b is absent
        [
                FeatureTokenType.VALUE, ["id"], "x", Type.STRING,
                FeatureTokenType.VALUE, ["a"], "longvalue", Type.STRING
        ].forEach(token -> buffer.push(token))
        def mapping = SchemaMapping.of(SCHEMA_ABSENT)
        int posA = mapping.getPositionsForTargetPath(["a"]).get(0)
        int posB = mapping.getPositionsForTargetPath(["b"]).get(0)

        when:
        // read then shrink a's slice (delta < 0), which shifts every later position
        buffer.getSlice(posA)
        buffer.replaceSlice(posA, [])
        // b is absent; its slice must still resolve to an empty, in-range range
        List<Object> sliceB = buffer.getSlice(posB)

        then:
        noExceptionThrown()
        sliceB.isEmpty()
    }

    def "a slice spans only its own property when its fragments are split around another"() {
        given:
        FeatureEventBuffer buffer = Util.createBuffer(SCHEMA, [])
        buffer.reset("test")
        SOURCE.forEach(token -> buffer.push(token))
        int posA = SchemaMapping.of(SCHEMA).getPositionsForTargetPath(["arrA"]).get(0)

        when:
        List<Object> slice = buffer.getSlice(posA)

        then:
        // both arrA fragments are present (the slice gathers the whole property) ...
        slice.contains("1")
        slice.contains("2")
        // ... and the unrelated arrB is NOT swallowed into arrA's slice
        !slice.contains(["arrB"])
        !slice.contains("b")
    }
}
