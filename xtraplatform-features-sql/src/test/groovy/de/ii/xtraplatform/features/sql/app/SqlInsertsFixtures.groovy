/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app


import de.ii.xtraplatform.features.sql.domain.SchemaSql
import de.ii.xtraplatform.features.sql.domain.SqlRelation
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.sql.domain.ImmutableSchemaSql
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlRelation

class SqlInsertsFixtures {

    static final SchemaSql MAIN_SCHEMA =
            new ImmutableSchemaSql.Builder()
                    .name("osirisobjekt")
                    .type(SchemaBase.Type.OBJECT)
                    .primaryKey(Optional.of("id"))
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("id")
                            .type(SchemaBase.Type.STRING)
                            .role(SchemaBase.Role.ID)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("kennung")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bezeichnung")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("veroeffentlichtam")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("verantwortlichestelle")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkung")
                            .type(SchemaBase.Type.STRING)
                            .build())

                    .build()

    static final String MAIN_EXPECTED =
            "INSERT INTO osirisobjekt (kennung,bezeichnung,veroeffentlichtam,verantwortlichestelle,bemerkung) VALUES ('1','2','3','4','5') RETURNING id;"

    static final String MAIN_WITH_ID_EXPECTED =
            "INSERT INTO osirisobjekt (id,kennung,bezeichnung,veroeffentlichtam,verantwortlichestelle,bemerkung) VALUES ('100','1','2','3','4','5') RETURNING id;"


    static final SchemaSql MERGE_SCHEMA =
            new ImmutableSchemaSql.Builder()
                    .name("fundorttiere")
                    .type(SchemaBase.Type.OBJECT)
                    .primaryKey(Optional.of("id"))
                    .addParentPath("osirisobjekt")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("tierart")
                            .type(SchemaBase.Type.INTEGER)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkungtierart")
                            .type(SchemaBase.Type.INTEGER)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("vorkommen")
                            .type(SchemaBase.Type.INTEGER)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("nachweis")
                            .type(SchemaBase.Type.INTEGER)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("erfassungsmethode")
                            .type(SchemaBase.Type.INTEGER)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkungpopulationsstadium")
                            .type(SchemaBase.Type.INTEGER)
                            .build())
                    .addRelation(new ImmutableSqlRelation.Builder()
                            .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                            .sourceContainer("osirisobjekt")
                            .sourceField("id")
                            .targetContainer("fundorttiere")
                            .targetField("id")
                            .build())
                    .build()

    static final String MERGE_EXPECTED =
            "INSERT INTO fundorttiere (id,tierart,bemerkungtierart,vorkommen,nachweis,erfassungsmethode,bemerkungpopulationsstadium) VALUES (100,16,17,18,19,20,21) RETURNING id;"


    static final SchemaSql MAIN_M_2_N_SCHEMA =
            new ImmutableSchemaSql.Builder()
                    .name("foto")
                    .type(SchemaBase.Type.OBJECT_ARRAY)
                    .primaryKey(Optional.of("id"))
                    .addParentPath("osirisobjekt")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("fotoverweis")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("hauptfoto")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("aufnahmezeitpunkt")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkung")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addRelation(new ImmutableSqlRelation.Builder()
                            .cardinality(SqlRelation.CARDINALITY.M_2_N)
                            .sourceContainer("osirisobjekt")
                            .sourceField("id")
                            .junctionSource("osirisobjekt_id")
                            .junction("osirisobjekt_2_foto")
                            .junctionTarget("foto_id")
                            .targetContainer("foto")
                            .targetField("id")
                            .build())
                    .build()

    static final List<String> MAIN_M_2_N_EXPECTED = [
            "INSERT INTO foto (fotoverweis,hauptfoto,aufnahmezeitpunkt,bemerkung) VALUES (6,7,8,9) RETURNING id;",
            "INSERT INTO foto (fotoverweis,hauptfoto,aufnahmezeitpunkt,bemerkung) VALUES (60,70,80,90) RETURNING id;"
    ]


    static final SchemaSql MERGE_MERGE_SCHEMA =
            new ImmutableSchemaSql.Builder()
                    .name("artbeobachtung")
                    .type(SchemaBase.Type.OBJECT)
                    .primaryKey(Optional.of("id"))
                    .addParentPath("osirisobjekt", "[id=id]fundorttiere")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("anzahl")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("begehungsmethode")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkunginformationsquelle")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("beobachtetam")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("haeufigkeit")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("informationsquelle")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("letzteskartierdatum")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("unschaerfe")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkungort")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addRelation(new ImmutableSqlRelation.Builder()
                            .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                            .sourceContainer("fundorttiere")
                            .sourceField("id")
                            .targetContainer("artbeobachtung")
                            .targetField("id")
                            .build())
                    .build()

    static final String MERGE_MERGE_EXPECTED =
            "INSERT INTO artbeobachtung (id,anzahl,begehungsmethode,bemerkunginformationsquelle,beobachtetam,haeufigkeit,informationsquelle,letzteskartierdatum,unschaerfe,bemerkungort) VALUES (100,22,23,24,25,26,27,28,29,30) RETURNING id;"


    static final SchemaSql MERGE_MERGE_ONE_2_ONE_SCHEMA =
            new ImmutableSchemaSql.Builder()
                    .name("geom")
                    .type(SchemaBase.Type.OBJECT)
                    .primaryKey(Optional.of("id"))
                    .addParentPath("osirisobjekt", "[id=id]fundorttiere", "[id=id]artbeobachtung")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("ST_AsBinary(ST_ForcePolygonCCW(geom))")
                            .type(SchemaBase.Type.GEOMETRY)
                            .build())
                    .addRelation(new ImmutableSqlRelation.Builder()
                            .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                            .sourceContainer("artbeobachtung")
                            .sourceField("geom")
                            .sourceSortKey("id")
                            .targetContainer("geom")
                            .targetField("id")
                            .build())
                    .build()

    static final String MERGE_MERGE_ONE_2_ONE_EXPECTED =
            "INSERT INTO geom (geom) VALUES (ST_ForcePolygonCW(ST_GeomFromText(31,25832))) RETURNING id;"

    static final String MERGE_MERGE_ONE_2_ONE_FOREIGN_KEY_EXPECTED =
            "UPDATE artbeobachtung SET geom=800 WHERE id=100 RETURNING null;"


    static final SchemaSql MERGE_MERGE_M_2_N_SCHEMA =
            new ImmutableSchemaSql.Builder()
                    .name("erfasser")
                    .type(SchemaBase.Type.OBJECT_ARRAY)
                    .primaryKey(Optional.of("id"))
                    .addParentPath("osirisobjekt", "[id=id]fundorttiere", "[id=id]artbeobachtung")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("name")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("bemerkung")
                            .type(SchemaBase.Type.STRING)
                            .build())
                    .addRelation(new ImmutableSqlRelation.Builder()
                            .cardinality(SqlRelation.CARDINALITY.M_2_N)
                            .sourceContainer("artbeobachtung")
                            .sourceField("id")
                            .junctionSource("artbeobachtung_id")
                            .junction("artbeobachtung_2_erfasser")
                            .junctionTarget("erfasser_id")
                            .targetContainer("erfasser")
                            .targetField("id")
                            .build())
                    .build()

    static final List<String> MERGE_MERGE_M_2_N_EXPECTED = [
            "INSERT INTO erfasser (name,bemerkung) VALUES (32,33) RETURNING id;",
            "INSERT INTO erfasser (name,bemerkung) VALUES (34,35) RETURNING id;"
    ]

    static final List<String> MERGE_MERGE_M_2_N_JUNCTION_EXPECTED = [
            "INSERT INTO artbeobachtung_2_erfasser (artbeobachtung_id,erfasser_id) VALUES (100,600) RETURNING null;",
            "INSERT INTO artbeobachtung_2_erfasser (artbeobachtung_id,erfasser_id) VALUES (100,700) RETURNING null;"
    ]


    static final SchemaSql MERGE_MERGE_WITH_CHILDREN =
            new ImmutableSchemaSql.Builder()
                    .from(MERGE_MERGE_SCHEMA)
                    .addAllPropertiesBuilders(new ImmutableSchemaSql.Builder()
                            .from(MERGE_MERGE_ONE_2_ONE_SCHEMA))
                    .addAllPropertiesBuilders(new ImmutableSchemaSql.Builder()
                            .from(MERGE_MERGE_M_2_N_SCHEMA))
                    .build()

    static final SchemaSql MERGE_WITH_CHILDREN =
            new ImmutableSchemaSql.Builder()
                    .from(MERGE_SCHEMA)
                    .addProperties(MERGE_MERGE_WITH_CHILDREN)
                    .build()

    static final SchemaSql FULL =
            new ImmutableSchemaSql.Builder()
                    .from(MAIN_SCHEMA)
                    .addProperties(MERGE_WITH_CHILDREN)
                    .addAllPropertiesBuilders(new ImmutableSchemaSql.Builder()
                            .from(MAIN_M_2_N_SCHEMA))
                    .build()

}
