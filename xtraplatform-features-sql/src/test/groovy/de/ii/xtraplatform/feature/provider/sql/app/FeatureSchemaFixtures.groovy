/*
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.provider.sql.app

import com.google.common.collect.ImmutableList
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.FeatureStoreInstanceContainer
import de.ii.xtraplatform.features.domain.FeatureStoreRelation
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreAttribute
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreInstanceContainer
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreRelatedContainer
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreRelation
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaBase.Type

class FeatureSchemaFixtures {

    static FeatureSchema VALUE_ARRAY = new ImmutableFeatureSchema.Builder()
            .name("externalprovider")
            .sourcePath("/externalprovider")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("id")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("externalprovidername", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=externalprovider_fk]externalprovider_externalprovidername/externalprovidername")
                    .type(Type.VALUE_ARRAY)
                    .valueType(Type.STRING))
            .build()

    static FeatureSchema OBJECT_ARRAY = new ImmutableFeatureSchema.Builder()
            .name("explorationsite")
            .sourcePath("/explorationsite")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("id")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("task", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=explorationsite_fk]explorationsite_task/[task_fk=id]task")
                    .type(Type.OBJECT_ARRAY)
                    .objectType("Link")
                    .putProperties2("title", new ImmutableFeatureSchema.Builder()
                            .sourcePath("projectname")
                            .type(Type.STRING))
                    .putProperties2("href", new ImmutableFeatureSchema.Builder()
                            .sourcePath("id")
                            .type(Type.STRING)))
            .build()


    static FeatureSchema MERGE = new ImmutableFeatureSchema.Builder()
            .name("eignungsflaeche")
            .sourcePath("/eignungsflaeche")
            .type(Type.OBJECT)
            .putProperties2("programm", new ImmutableFeatureSchema.Builder()
                    .sourcePath("programm")
                    .type(Type.STRING))
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=id]osirisobjekt/id")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("kennung", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=id]osirisobjekt/kennung")
                    .type(Type.STRING))
            .build()

    static FeatureSchema SELF_JOINS = new ImmutableFeatureSchema.Builder()
            .name("building")
            .sourcePath("/building")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("id")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("consistsOfBuildingPart", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=fk_buildingpart_parent]building")
                    .type(Type.OBJECT_ARRAY)
                    .putProperties2("href", new ImmutableFeatureSchema.Builder()
                            .sourcePath("id")
                            .type(Type.STRING)))
            .putProperties2("parent", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[fk_buildingpart_parent=id]building")
                    .type(Type.OBJECT)
                    .putProperties2("href", new ImmutableFeatureSchema.Builder()
                            .sourcePath("id")
                            .type(Type.STRING)))
            .build()

    static FeatureSchema SELF_JOINS_FILTER = new ImmutableFeatureSchema.Builder()
            .name("building")
            .sourcePath("/building{filter=id>1}")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("oid")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("consistsOfBuildingPart", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=fk_buildingpart_parent]building{filter=href>100}")
                    .type(Type.OBJECT_ARRAY)
                    .putProperties2("href", new ImmutableFeatureSchema.Builder()
                            .sourcePath("id")
                            .type(Type.STRING)))
            .putProperties2("parent", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[fk_buildingpart_parent=id]building{filter=href>1000}")
                    .type(Type.OBJECT)
                    .putProperties2("href", new ImmutableFeatureSchema.Builder()
                            .sourcePath("id")
                            .type(Type.STRING)))
            .build()

    static FeatureSchema SELF_JOIN_NESTED_DUPLICATE = new ImmutableFeatureSchema.Builder()
            .name("building")
            .sourcePath("/building")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("id")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("genericAttributesString", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=fk_feature]att_string_building")
                    .type(Type.OBJECT_ARRAY)
                    .putProperties2("name", new ImmutableFeatureSchema.Builder()
                            .sourcePath("name")
                            .type(Type.STRING)))
            .putProperties2("consistsOfBuildingPart", new ImmutableFeatureSchema.Builder()
                    .sourcePath("[id=fk_buildingpart_parent]building")
                    .type(Type.OBJECT_ARRAY)
                    .putProperties2("genericAttributesString", new ImmutableFeatureSchema.Builder()
                            .sourcePath("[id=fk_feature]att_string_building")
                            .type(Type.OBJECT_ARRAY)
                            .putProperties2("name", new ImmutableFeatureSchema.Builder()
                                    .sourcePath("name")
                                    .type(Type.STRING))))
            .build()

    static FeatureSchema PROPERTY_WITH_MULTIPLE_SOURCE_PATHS = new ImmutableFeatureSchema.Builder()
            .name("address")
            .sourcePath("/o12006")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("objid")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("component", new ImmutableFeatureSchema.Builder()
                    .addSourcePaths("lan")
                    .addSourcePaths("rbz")
                    .addSourcePaths("krs")
                    .addSourcePaths("gmd")
                    .type(Type.STRING))
            .build()

    //TODO: nested

    //TODO: objects without sourcePath
    static FeatureSchema OBJECT_WITHOUT_SOURCE_PATH = new ImmutableFeatureSchema.Builder()
            .name("explorationsite")
            .sourcePath("/explorationsite")
            .type(Type.OBJECT)
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .sourcePath("id")
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID))
            .putProperties2("legalAvailability", new ImmutableFeatureSchema.Builder()
                    .type(Type.OBJECT)
                    .objectType("Link")
                    .putProperties2("title", new ImmutableFeatureSchema.Builder()
                            .sourcePath("legalavailability_fk")
                            .type(Type.STRING))
                    .putProperties2("href", new ImmutableFeatureSchema.Builder()
                            .sourcePath("legalavailability_fk")
                            .type(Type.STRING)))
            .build()

    //TODO: flags


}
