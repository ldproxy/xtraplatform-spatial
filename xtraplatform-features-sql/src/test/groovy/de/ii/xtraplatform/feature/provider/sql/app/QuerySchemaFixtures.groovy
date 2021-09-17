/*
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.provider.sql.app


import de.ii.xtraplatform.cql.domain.CqlFilter
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.Gt
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.feature.provider.sql.domain.ImmutableSchemaSql
import de.ii.xtraplatform.feature.provider.sql.domain.ImmutableSqlRelation
import de.ii.xtraplatform.feature.provider.sql.domain.SchemaSql
import de.ii.xtraplatform.feature.provider.sql.domain.SqlRelation
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaBase.Type

class QuerySchemaFixtures {

    static List<SchemaSql> SIMPLE = [
            new ImmutableSchemaSql.Builder()
                    .name("externalprovider")
                    .sourcePath("externalprovider")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("id")
                            .type(Type.STRING)
                            .sourcePath("id")
                            .parentPath(["externalprovider"])
                            .role(SchemaBase.Role.ID)
                            .build())
                    .build()
    ]

    static List<SchemaSql> SIMPLE_FILTER = [
            new ImmutableSchemaSql.Builder()
                    .name("externalprovider")
                    .sourcePath("externalprovider")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .filter(CqlFilter.of(Eq.of("type", ScalarLiteral.of(1))))
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("type")
                            .type(Type.INTEGER)
                            .sourcePath("type")
                            .parentPath(["externalprovider"])
                            .build())
                    .build()
    ]

    static List<SchemaSql> VALUE_ARRAY = [
            new ImmutableSchemaSql.Builder()
                    .name("externalprovider")
                    .sourcePath("externalprovider")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("id")
                            .type(Type.STRING)
                            .sourcePath("id")
                            .parentPath(["externalprovider"])
                            .role(SchemaBase.Role.ID)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("externalprovider_externalprovidername")
                            .type(Type.OBJECT_ARRAY)
                            .parentPath(["externalprovider"])
                            .sortKey("id")
                            .primaryKey("id")
                            .addRelation(new ImmutableSqlRelation.Builder()
                                    .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                    .sourceContainer("externalprovider")
                                    .sourceField("id")
                                    .targetContainer("externalprovider_externalprovidername")
                                    .targetField("externalprovider_fk")
                                    .build())
                            .addProperties(new ImmutableSchemaSql.Builder()
                                    .name("externalprovidername")
                                    .type(Type.STRING)
                                    .sourcePath("externalprovidername")
                                    .parentPath(["externalprovider", "[id=externalprovider_fk]externalprovider_externalprovidername"])
                                    .build())
                            .build())
                    .build()
    ]


    static List<SchemaSql> OBJECT_ARRAY = [
            new ImmutableSchemaSql.Builder()
                    .name("explorationsite")
                    .sourcePath("explorationsite")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("id")
                            .type(Type.STRING)
                            .sourcePath("id")
                            .parentPath(["explorationsite"])
                            .role(SchemaBase.Role.ID)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("task")
                            .sourcePath("task")
                            .type(Type.OBJECT_ARRAY)
                            .parentPath(["explorationsite"])
                            .sortKey("id")
                            .primaryKey("id")
                            .addRelation(new ImmutableSqlRelation.Builder()
                                    .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                    .sourceContainer("explorationsite")
                                    .sourceField("id")
                                    .targetContainer("explorationsite_task")
                                    .targetField("explorationsite_fk")
                                    .build())
                            .addRelation(new ImmutableSqlRelation.Builder()
                                    .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                                    .sourceContainer("explorationsite_task")
                                    .sourceField("task_fk")
                                    .sourceSortKey("id")
                                    .sourcePrimaryKey("id")
                                    .targetContainer("task")
                                    .targetField("id")
                                    .build())
                            .addProperties(new ImmutableSchemaSql.Builder()
                                    .name("projectname")
                                    .type(Type.STRING)
                                    .sourcePath("title")
                                    .parentPath(["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"])
                                    .build())
                            .addProperties(new ImmutableSchemaSql.Builder()
                                    .name("id")
                                    .type(Type.STRING)
                                    .sourcePath("href")
                                    .parentPath(["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"])
                                    .build())
                            .build())
                    .build()
    ]

    static List<SchemaSql> MERGE = [
            new ImmutableSchemaSql.Builder()
                    .name("eignungsflaeche")
                    .sourcePath("eignungsflaeche")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("programm")
                            .type(Type.STRING)
                            .sourcePath("programm")
                            .parentPath(["eignungsflaeche"])
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("osirisobjekt")
                            .type(Type.OBJECT)
                            .parentPath(["eignungsflaeche"])
                            .sortKey("id")
                            .primaryKey("id")
                            .addRelation(new ImmutableSqlRelation.Builder()
                                    .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                                    .sourceContainer("eignungsflaeche")
                                    .sourceField("id")
                                    .targetContainer("osirisobjekt")
                                    .targetField("id")
                                    .build())
                            .addProperties(new ImmutableSchemaSql.Builder()
                                    .name("id")
                                    .type(Type.STRING)
                                    .sourcePath("id")
                                    .parentPath(["eignungsflaeche", "[id=id]osirisobjekt"])
                                    .role(SchemaBase.Role.ID)
                                    .build())
                            .addProperties(new ImmutableSchemaSql.Builder()
                                    .name("kennung")
                                    .type(Type.STRING)
                                    .sourcePath("kennung")
                                    .parentPath(["eignungsflaeche", "[id=id]osirisobjekt"])
                                    .build())
                            .build())
                    .build()
    ]


    static SchemaSql SIMPLE_GEOMETRY = new ImmutableSchemaSql.Builder()
            .name("building")
            .type(Type.OBJECT)
            .sortKey("id")
            .addProperties(new ImmutableSchemaSql.Builder()
                    .name("location")
                    .sourcePath("location")
                    .type(Type.GEOMETRY)
                    .parentPath(["building"])
                    .build())
            .build()

    static SchemaSql JOINED_GEOMETRY = new ImmutableSchemaSql.Builder()
            .name("building")
            .type(Type.OBJECT)
            .sortKey("id")
            .addProperties(new ImmutableSchemaSql.Builder()
                    .name("geometry")
                    .type(Type.OBJECT)
                    .parentPath(["building"])
                    .addRelation(new ImmutableSqlRelation.Builder()
                            .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                            .sourceContainer("building")
                            .sourceField("id")
                            .targetContainer("geometry")
                            .targetField("id")
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("location")
                            .sourcePath("location")
                            .type(Type.GEOMETRY)
                            .parentPath(["building", "geometry"])
                            .build())
                    .build())
            .build();

    static SchemaSql SIMPLE_INSTANT = new ImmutableSchemaSql.Builder()
            .name("building")
            .type(Type.OBJECT)
            .sortKey("id")
            .addProperties(new ImmutableSchemaSql.Builder()
                    .name("built")
                    .sourcePath("built")
                    .type(Type.DATETIME)
                    .parentPath(["building"])
                    .build())
            .build()

    static SchemaSql SIMPLE_INTERVAL = new ImmutableSchemaSql.Builder()
            .name("building")
            .type(Type.OBJECT)
            .sortKey("id")
            .addProperties(new ImmutableSchemaSql.Builder()
                    .name("updated")
                    .sourcePath("updated")
                    .type(Type.DATETIME)
                    .parentPath(["building"])
                    .build())
            .build()

    static List<SchemaSql> SELF_JOINS = [new ImmutableSchemaSql.Builder()
                                                 .name("building")
                                                 .type(Type.OBJECT)
                                                 .sourcePath("building")
                                                 .sortKey("id")
                                                 .primaryKey("id")
                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                         .name("id")
                                                         .sourcePath("id")
                                                         .type(Type.STRING)
                                                         .role(SchemaBase.Role.ID)
                                                         .parentPath(["building"])
                                                         .build())
                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                         .name("building")
                                                         .type(Type.OBJECT_ARRAY)
                                                         .sourcePath("consistsOfBuildingPart")
                                                         .sortKey("id")
                                                         .primaryKey("id")
                                                         .parentPath(["building"])
                                                         .addRelation(new ImmutableSqlRelation.Builder()
                                                                 .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                                                 .sourceContainer("building")
                                                                 .sourceField("id")
                                                                 .sourceSortKey("id")
                                                                 .targetContainer("building")
                                                                 .targetField("fk_buildingpart_parent")
                                                                 .build())
                                                         .addProperties(new ImmutableSchemaSql.Builder()
                                                                 .name("id")
                                                                 .sourcePath("href")
                                                                 .type(Type.STRING)
                                                                 .parentPath(["building", "[id=fk_buildingpart_parent]building"])
                                                                 .build())
                                                         .build())
                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                         .name("building")
                                                         .type(Type.OBJECT)
                                                         .sourcePath("parent")
                                                         .sortKey("id")
                                                         .primaryKey("id")
                                                         .parentPath(["building"])
                                                         .addRelation(new ImmutableSqlRelation.Builder()
                                                                 .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                                                                 .sourceContainer("building")
                                                                 .sourceField("fk_buildingpart_parent")
                                                                 .sourceSortKey("id")
                                                                 .sourcePrimaryKey("id")
                                                                 .targetContainer("building")
                                                                 .targetField("id")
                                                                 .build())
                                                         .addProperties(new ImmutableSchemaSql.Builder()
                                                                 .name("id")
                                                                 .sourcePath("href")
                                                                 .type(Type.STRING)
                                                                 .parentPath(["building", "[fk_buildingpart_parent=id]building"])
                                                                 .build())
                                                         .build())
                                                 .build()]

    static List<SchemaSql> SELF_JOINS_FILTER = [new ImmutableSchemaSql.Builder()
                                                        .name("building")
                                                        .type(Type.OBJECT)
                                                        .sourcePath("building")
                                                        .sortKey("id")
                                                        .primaryKey("id")
                                                        .filter(CqlFilter.of(Gt.of("id", ScalarLiteral.of(1))))
                                                        .filterString("id>1")
                                                        .addProperties(new ImmutableSchemaSql.Builder()
                                                                .name("oid")
                                                                .sourcePath("id")
                                                                .type(Type.STRING)
                                                                .role(SchemaBase.Role.ID)
                                                                .parentPath(["building{filter=id>1}"])
                                                                .build())
                                                        .addProperties(new ImmutableSchemaSql.Builder()
                                                                .name("building")
                                                                .type(Type.OBJECT_ARRAY)
                                                                .sourcePath("consistsOfBuildingPart")
                                                                .sortKey("id")
                                                                .primaryKey("id")
                                                                .parentPath(["building{filter=id>1}"])
                                                                .filter(CqlFilter.of(Gt.of("href", ScalarLiteral.of(100))))
                                                                .filterString("href>100")
                                                                .addRelation(new ImmutableSqlRelation.Builder()
                                                                        .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                                                        .sourceContainer("building")
                                                                        .sourceField("id")
                                                                        .sourceSortKey("id")
                                                                        .sourceFilter("id > 1")
                                                                        .targetContainer("building")
                                                                        .targetField("fk_buildingpart_parent")
                                                                        .targetFilter("href > 100")
                                                                        .build())
                                                                .addProperties(new ImmutableSchemaSql.Builder()
                                                                        .name("id")
                                                                        .sourcePath("href")
                                                                        .type(Type.STRING)
                                                                        .parentPath(["building{filter=id>1}", "[id=fk_buildingpart_parent]building{filter=href>100}"])
                                                                        .build())
                                                                .build())
                                                        .addProperties(new ImmutableSchemaSql.Builder()
                                                                .name("building")
                                                                .type(Type.OBJECT)
                                                                .sourcePath("parent")
                                                                .sortKey("id")
                                                                .primaryKey("id")
                                                                .parentPath(["building{filter=id>1}"])
                                                                .filter(CqlFilter.of(Gt.of("href", ScalarLiteral.of(1000))))
                                                                .filterString("href>1000")
                                                                .addRelation(new ImmutableSqlRelation.Builder()
                                                                        .cardinality(SqlRelation.CARDINALITY.ONE_2_ONE)
                                                                        .sourceContainer("building")
                                                                        .sourceField("fk_buildingpart_parent")
                                                                        .sourceSortKey("id")
                                                                        .sourcePrimaryKey("id")
                                                                        .sourceFilter("id > 1")
                                                                        .targetContainer("building")
                                                                        .targetField("id")
                                                                        .targetFilter("href > 1000")
                                                                        .build())
                                                                .addProperties(new ImmutableSchemaSql.Builder()
                                                                        .name("id")
                                                                        .sourcePath("href")
                                                                        .type(Type.STRING)
                                                                        .parentPath(["building{filter=id>1}", "[fk_buildingpart_parent=id]building{filter=href>1000}"])
                                                                        .build())
                                                                .build())
                                                        .build()]

    static List<SchemaSql> SELF_JOIN_NESTED_DUPLICATE = [new ImmutableSchemaSql.Builder()
                                                                 .name("building")
                                                                 .type(Type.OBJECT)
                                                                 .sourcePath("building")
                                                                 .sortKey("id")
                                                                 .primaryKey("id")
                                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                                         .name("id")
                                                                         .sourcePath("id")
                                                                         .type(Type.STRING)
                                                                         .role(SchemaBase.Role.ID)
                                                                         .parentPath(["building"])
                                                                         .build())
                                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                                         .name("att_string_building")
                                                                         .type(Type.OBJECT_ARRAY)
                                                                         .sourcePath("genericAttributesString")
                                                                         .sortKey("id")
                                                                         .primaryKey("id")
                                                                         .parentPath(["building"])
                                                                         .addRelation(new ImmutableSqlRelation.Builder()
                                                                                 .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                                                                 .sourceContainer("building")
                                                                                 .sourceField("id")
                                                                                 .sourceSortKey("id")
                                                                                 .sourcePrimaryKey("id")
                                                                                 .targetContainer("att_string_building")
                                                                                 .targetField("fk_feature")
                                                                                 .build())
                                                                         .addProperties(new ImmutableSchemaSql.Builder()
                                                                                 .name("name")
                                                                                 .sourcePath("name")
                                                                                 .type(Type.STRING)
                                                                                 .parentPath(["building", "[id=fk_feature]att_string_building"])
                                                                                 .build())
                                                                         .build())
                                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                                         .name("building")
                                                                         .type(Type.OBJECT_ARRAY)
                                                                         .sourcePath("consistsOfBuildingPart")
                                                                         .sortKey("id")
                                                                         .primaryKey("id")
                                                                         .parentPath(["building"])
                                                                         .addRelation(new ImmutableSqlRelation.Builder()
                                                                                 .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                                                                 .sourceContainer("building")
                                                                                 .sourceField("id")
                                                                                 .sourceSortKey("id")
                                                                                 .targetContainer("building")
                                                                                 .targetField("fk_buildingpart_parent")
                                                                                 .build())
                                                                         .addProperties(new ImmutableSchemaSql.Builder()
                                                                                 .name("att_string_building")
                                                                                 .type(Type.OBJECT_ARRAY)
                                                                                 .sourcePath("genericAttributesString")
                                                                                 .sortKey("id")
                                                                                 .primaryKey("id")
                                                                                 .parentPath(["building"])
                                                                                 .addRelation(new ImmutableSqlRelation.Builder()
                                                                                         .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                                                                         .sourceContainer("building")
                                                                                         .sourceField("id")
                                                                                         .sourceSortKey("id")
                                                                                         .targetContainer("building")
                                                                                         .targetField("fk_buildingpart_parent")
                                                                                         .build())
                                                                                 .addRelation(new ImmutableSqlRelation.Builder()
                                                                                         .cardinality(SqlRelation.CARDINALITY.ONE_2_N)
                                                                                         .sourceContainer("building")
                                                                                         .sourceField("id")
                                                                                         .sourceSortKey("id")
                                                                                         .sourcePrimaryKey("id")
                                                                                         .targetContainer("att_string_building")
                                                                                         .targetField("fk_feature")
                                                                                         .build())
                                                                                 .addProperties(new ImmutableSchemaSql.Builder()
                                                                                         .name("name")
                                                                                         .sourcePath("name")
                                                                                         .type(Type.STRING)
                                                                                         .parentPath(["building", "[id=fk_buildingpart_parent]building", "[id=fk_feature]att_string_building"])
                                                                                         .build())
                                                                                 .build())
                                                                         .build())
                                                                 .build()]

    static List<SchemaSql> OBJECT_WITHOUT_SOURCE_PATH = [
            new ImmutableSchemaSql.Builder()
                    .name("explorationsite")
                    .sourcePath("explorationsite")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("id")
                            .type(Type.STRING)
                            .sourcePath("id")
                            .parentPath(["explorationsite"])
                            .role(SchemaBase.Role.ID)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("legalavailability_fk")
                            .type(Type.STRING)
                            .sourcePath("legalAvailability.title")
                            .parentPath(["explorationsite"])
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("legalavailability_fk")
                            .type(Type.STRING)
                            .sourcePath("legalAvailability.href")
                            .parentPath(["explorationsite"])
                            .build())
                    .build()
    ]

    static List<SchemaSql> PROPERTY_WITH_MULTIPLE_SOURCE_PATHS = [
            new ImmutableSchemaSql.Builder()
                    .name("o12006")
                    .sourcePath("address")
                    .type(Type.OBJECT)
                    .sortKey("id")
                    .primaryKey("id")
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("objid")
                            .type(Type.STRING)
                            .sourcePath("id")
                            .parentPath(["o12006"])
                            .role(SchemaBase.Role.ID)
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("lan")
                            .type(Type.STRING)
                            .sourcePath("component")
                            .parentPath(["o12006"])
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("rbz")
                            .type(Type.STRING)
                            .sourcePath("component")
                            .parentPath(["o12006"])
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("krs")
                            .type(Type.STRING)
                            .sourcePath("component")
                            .parentPath(["o12006"])
                            .build())
                    .addProperties(new ImmutableSchemaSql.Builder()
                            .name("gmd")
                            .type(Type.STRING)
                            .sourcePath("component")
                            .parentPath(["o12006"])
                            .build())
                    .build()
    ]

}
