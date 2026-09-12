/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.ImmutableInResultSetByKey
import de.ii.xtraplatform.cql.domain.InResultSetByKey
import de.ii.xtraplatform.cql.domain.IsNull
import de.ii.xtraplatform.cql.domain.Not
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRulesDeriver
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaBase.Type
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.Function

class FilterEncoderSqlInResultSetByKeySpec extends Specification {

    @Shared
    Map<String, SqlQueryMapping> mappings = [:]
    @Shared
    FilterEncoderSql filterEncoder

    // the key of AX_Gemeinde: four parts of the same type, which a positional key could transpose
    static final Map<String, Property> KEY = [
            land            : Property.of("gkz_lan"),
            regierungsbezirk: Property.of("gkz_rbz"),
            kreis           : Property.of("gkz_krs"),
            gemeinde        : Property.of("gkz_gem")]
    static final Map<String, String> PRODUCER_KEY = [
            land            : "gmd_lan",
            regierungsbezirk: "gmd_rbz",
            kreis           : "gmd_krs",
            gemeinde        : "gmd_gmd"]

    def setupSpec() {
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(new ImmutableSqlPathDefaults.Builder().build(), cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        def resolver = new MappingOperationResolver()
        def rulesDeriver = new MappingRulesDeriver()

        def prop = { String sourcePath -> new ImmutableFeatureSchema.Builder().sourcePath(sourcePath).type(Type.STRING) }

        def producer = new ImmutableFeatureSchema.Builder()
                .name("flurstueck").sourcePath("/o11001").type(Type.OBJECT)
                .putProperties2("id", prop("objid").role(SchemaBase.Role.ID))
                .putProperties2("gmd_lan", prop("gdz__lan"))
                .putProperties2("gmd_rbz", prop("gdz__rbz"))
                .putProperties2("gmd_krs", prop("gdz__krs"))
                .putProperties2("gmd_gmd", prop("gdz__gem"))
                .putProperties2("mat", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o11001__mat/stm").type(Type.VALUE_ARRAY).valueType(Type.STRING))
                .build()

        def consumer = new ImmutableFeatureSchema.Builder()
                .name("gemeinde").sourcePath("/o73005").type(Type.OBJECT)
                .putProperties2("id", prop("objid").role(SchemaBase.Role.ID))
                .putProperties2("gkz_lan", prop("gkz__lan"))
                .putProperties2("gkz_rbz", prop("gkz__rbz"))
                .putProperties2("gkz_krs", prop("gkz__krs"))
                .putProperties2("gkz_gem", prop("gkz__gem"))
                .putProperties2("mat", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o73005__mat/stm").type(Type.VALUE_ARRAY).valueType(Type.STRING))
                .build()

        [producer, consumer].each { schema ->
            def resolved = schema.accept(resolver, List.of())
            mappings[schema.getName()] = mappingDeriver.derive(resolved.accept(rulesDeriver), resolved).get(0)
        }

        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, List.of(), null,
                { type -> Optional.ofNullable(mappings[type]) } as Function)
    }

    static InResultSetByKey resolved(Map<String, Property> key, Map<String, String> producerKey, Closure extra) {
        def node = InResultSetByKey.of(key, "fs_gemeinde")
        def builder = new ImmutableInResultSetByKey.Builder()
                .from(node)
                .args(node.getArgs())
                .producerType("flurstueck")
                .producerKey(producerKey)
        extra(builder)
        return builder.build()
    }

    static InResultSetByKey gemeinde(Closure extra = { }) {
        return resolved(KEY, PRODUCER_KEY, { builder ->
            builder.producerFilter(IsNull.of("gmd_lan"))
            extra(builder)
        })
    }

    def 'the producer is re-derived as a materialized CTE, one column per key part'() {

        when:
        def sql = filterEncoder.encode(gemeinde(), mappings["gemeinde"])

        then: 'both sides use the canonical part order, so the columns line up by name, not by position'
        sql == "EXISTS (WITH _rs_0_fs_gemeinde AS MATERIALIZED (SELECT DISTINCT A.gdz__gem AS rs_value_0," +
                " A.gdz__krs AS rs_value_1, A.gdz__lan AS rs_value_2, A.gdz__rbz AS rs_value_3" +
                " FROM o11001 A WHERE A.gdz__lan IS NULL)" +
                " SELECT 1 FROM _rs_0_fs_gemeinde _rsk WHERE _rsk.rs_value_0 = A.gkz__gem" +
                " AND _rsk.rs_value_1 = A.gkz__krs AND _rsk.rs_value_2 = A.gkz__lan" +
                " AND _rsk.rs_value_3 = A.gkz__rbz)"
    }

    def 'a materialized set is inlined as key tuples'() {

        when:
        def sql = filterEncoder.encode(
                gemeinde({ b -> b.materializedValues([["06", "4", "36", "006"], ["06", "4", "14", "000"]]) }),
                mappings["gemeinde"])

        then:
        sql == "EXISTS (SELECT 1 FROM (VALUES ('06', '4', '36', '006'), ('06', '4', '14', '000'))" +
                " _rsk (rs_value_0, rs_value_1, rs_value_2, rs_value_3)" +
                " WHERE _rsk.rs_value_0 = A.gkz__gem AND _rsk.rs_value_1 = A.gkz__krs" +
                " AND _rsk.rs_value_2 = A.gkz__lan AND _rsk.rs_value_3 = A.gkz__rbz)"
    }

    def 'an empty set can never match'() {

        when:
        def sql = filterEncoder.encode(gemeinde({ b -> b.materializedValues([]) }), mappings["gemeinde"])

        then:
        sql == "1 = 0"
    }

    def 'an oversized set is read from its materialized table'() {

        when:
        def sql = filterEncoder.encode(gemeinde({ b -> b.materializedTable("_rs_mat_x") }), mappings["gemeinde"])

        then:
        sql == "EXISTS (SELECT 1 FROM _rs_mat_x _rsk WHERE _rsk.rs_value_0 = A.gkz__gem" +
                " AND _rsk.rs_value_1 = A.gkz__krs AND _rsk.rs_value_2 = A.gkz__lan" +
                " AND _rsk.rs_value_3 = A.gkz__rbz)"
    }

    def 'the negation is NOT EXISTS, so a null key part does not drop the feature'() {

        given: 'with a row-constructor IN, a NULL in any key part would make NOT IN unknown'

        when:
        def sql = filterEncoder.encode(Not.of(gemeinde({ b -> b.materializedTable("_rs_mat_x") })), mappings["gemeinde"])

        then:
        sql.startsWith("NOT (EXISTS (")
        sql == "NOT (EXISTS (SELECT 1 FROM _rs_mat_x _rsk WHERE _rsk.rs_value_0 = A.gkz__gem" +
                " AND _rsk.rs_value_1 = A.gkz__krs AND _rsk.rs_value_2 = A.gkz__lan" +
                " AND _rsk.rs_value_3 = A.gkz__rbz))"
    }

    def 'the producer select for materialization returns the bare key columns'() {

        when:
        def sql = filterEncoder.encodeResultSetProducerByKey(gemeinde())

        then: 'DISTINCT, because the members of the set are the distinct keys, not the producer rows'
        sql == "SELECT DISTINCT A.gdz__gem, A.gdz__krs, A.gdz__lan, A.gdz__rbz FROM o11001 A WHERE A.gdz__lan IS NULL"
    }

    def 'the key part types are reported in canonical part order'() {

        when:
        def types = filterEncoder.resultSetValueTypesByKey(gemeinde())

        then:
        types == [Type.STRING, Type.STRING, Type.STRING, Type.STRING]
    }

    def 'a consumer key part that needs a join is rejected'() {

        given: 'a multi-valued property lives in a junction table, so it cannot take part in a key'
        def node = resolved([land: Property.of("mat")], [land: "gmd_lan"], { })

        when:
        filterEncoder.encode(node, mappings["gemeinde"])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("not on the main table of this feature type")
        e.message.contains("mat")
    }

    def 'a consumer key part the feature type does not have is rejected'() {

        given:
        def node = resolved([land: Property.of("nope")], [land: "gmd_lan"], { })

        when:
        filterEncoder.encode(node, mappings["gemeinde"])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("is unknown for this feature type")
        e.message.contains("nope")
    }

    def 'a key part the result set does not define is reported'() {

        given:
        def node = resolved(KEY, [land: "gmd_lan"], { })

        when:
        filterEncoder.encode(node, mappings["gemeinde"])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("does not define the key part")
    }

    def 'a producer key part that needs a join is rejected'() {

        given:
        def node = resolved([land: Property.of("gkz_lan")], [land: "mat"], { })

        when:
        filterEncoder.encode(node, mappings["gemeinde"])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("result set 'fs_gemeinde'")
        e.message.contains("not on the main table of feature type 'flurstueck'")
    }
}
