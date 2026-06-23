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
// them. The buffer stores tokens in provider order and the flush pass re-sorts them into schema
// order — otherwise a property declared before its scalar siblings ends up after them, producing
// XML/GML that does not match the application schema's element order.
class FeatureEventBufferOrderSpec extends Specification {

    static List<Object> run(FeatureSchema schema, List<Object> source) {
        List<Object> actual = []
        FeatureTokenReader reader = Util.createReader(schema, actual)
        source.forEach(token -> reader.onToken(token))
        return actual
    }

    // id, child (OBJECT on a joined table), a, b (scalar columns on the main table)
    static FeatureSchema TOPLEVEL = new ImmutableFeatureSchema.Builder()
            .name("t").sourcePath("/t").type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("objid").type(Type.STRING).role(SchemaBase.Role.ID))
            .putProperties2("child", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=rid]t__child").type(Type.OBJECT)
                    .putProperties2("value", new ImmutableFeatureSchema.Builder()
                            .sourcePath("value").type(Type.STRING)))
            .putProperties2("a", new ImmutableFeatureSchema.Builder().sourcePath("a").type(Type.STRING))
            .putProperties2("b", new ImmutableFeatureSchema.Builder().sourcePath("b").type(Type.STRING))
            .build()

    // provider order: the main-table scalars (a, b) arrive before the joined object (child)
    static List<Object> TOPLEVEL_SOURCE = [
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
    static List<Object> TOPLEVEL_EXPECTED = [
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
        when:
        List<Object> actual = run(TOPLEVEL, TOPLEVEL_SOURCE)

        then:
        actual == TOPLEVEL_EXPECTED
    }

    // outer object q; inner object dpl (on a joined table) declared before the inner scalars gst, vwl
    static FeatureSchema NESTED = new ImmutableFeatureSchema.Builder()
            .name("t").sourcePath("/t").type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("objid").type(Type.STRING).role(SchemaBase.Role.ID))
            .putProperties2("q", new ImmutableFeatureSchema.Builder()
                    .sourcePath("q").type(Type.OBJECT)
                    .putProperties2("dpl", new ImmutableFeatureSchema.Builder()
                            .sourcePath("[id=rid]q__dpl").type(Type.OBJECT)
                            .putProperties2("h", new ImmutableFeatureSchema.Builder()
                                    .sourcePath("h").type(Type.STRING)))
                    .putProperties2("gst", new ImmutableFeatureSchema.Builder().sourcePath("gst").type(Type.STRING))
                    .putProperties2("vwl", new ImmutableFeatureSchema.Builder().sourcePath("vwl").type(Type.STRING)))
            .build()

    // provider order: the inner main-table scalars (gst, vwl) arrive before the joined object (dpl)
    static List<Object> NESTED_SOURCE = [
            FeatureTokenType.INPUT, true, FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE, ["t", "objid"], "f1", Type.STRING,
            FeatureTokenType.OBJECT, ["t", "q"],
            FeatureTokenType.VALUE, ["t", "q", "gst"], "G", Type.STRING,
            FeatureTokenType.VALUE, ["t", "q", "vwl"], "V", Type.STRING,
            FeatureTokenType.OBJECT, ["t", "q", "[id=rid]q__dpl"],
            FeatureTokenType.VALUE, ["t", "q", "[id=rid]q__dpl", "h"], "H", Type.STRING,
            FeatureTokenType.OBJECT_END, ["t", "q", "[id=rid]q__dpl"],
            FeatureTokenType.OBJECT_END, ["t", "q"],
            FeatureTokenType.FEATURE_END, FeatureTokenType.INPUT_END
    ]

    // schema order: dpl is emitted before gst/vwl inside q (the case the cursor placement alone gets
    // wrong — it leaves dpl after gst/vwl; only the flush pass corrects it)
    static List<Object> NESTED_EXPECTED = [
            FeatureTokenType.INPUT, true, FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE, ["id"], "f1", Type.STRING,
            FeatureTokenType.OBJECT, ["q"],
            FeatureTokenType.OBJECT, ["q", "dpl"],
            FeatureTokenType.VALUE, ["q", "dpl", "h"], "H", Type.STRING,
            FeatureTokenType.OBJECT_END, ["q", "dpl"],
            FeatureTokenType.VALUE, ["q", "gst"], "G", Type.STRING,
            FeatureTokenType.VALUE, ["q", "vwl"], "V", Type.STRING,
            FeatureTokenType.OBJECT_END, ["q"],
            FeatureTokenType.FEATURE_END, FeatureTokenType.INPUT_END
    ]

    def "a nested joined object declared before its scalar siblings is emitted at its schema position"() {
        when:
        List<Object> actual = run(NESTED, NESTED_SOURCE)

        then:
        actual == NESTED_EXPECTED
    }

    // the provider produces a single-valued object backed by more than one table as several
    // per-table fragments: q first with its main-table scalars (gst, vwl), then again with the
    // joined object (dpl) - two OBJECT[q]..OBJECT_END[q] blocks
    static List<Object> SPLIT_SOURCE = [
            FeatureTokenType.INPUT, true, FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE, ["t", "objid"], "f1", Type.STRING,
            FeatureTokenType.OBJECT, ["t", "q"],
            FeatureTokenType.VALUE, ["t", "q", "gst"], "G", Type.STRING,
            FeatureTokenType.VALUE, ["t", "q", "vwl"], "V", Type.STRING,
            FeatureTokenType.OBJECT_END, ["t", "q"],
            FeatureTokenType.OBJECT, ["t", "q"],
            FeatureTokenType.OBJECT, ["t", "q", "[id=rid]q__dpl"],
            FeatureTokenType.VALUE, ["t", "q", "[id=rid]q__dpl", "h"], "H", Type.STRING,
            FeatureTokenType.OBJECT_END, ["t", "q", "[id=rid]q__dpl"],
            FeatureTokenType.OBJECT_END, ["t", "q"],
            FeatureTokenType.FEATURE_END, FeatureTokenType.INPUT_END
    ]

    // the fragments coalesce into one q object, its children in schema order (dpl, gst, vwl)
    def "object fragments split across tables coalesce into a single object"() {
        when:
        List<Object> actual = run(NESTED, SPLIT_SOURCE)

        then:
        actual == NESTED_EXPECTED
    }
}
