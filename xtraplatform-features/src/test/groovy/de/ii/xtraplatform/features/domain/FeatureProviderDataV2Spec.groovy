/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain


import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation
import de.ii.xtraplatform.geometries.domain.GeometryType
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

class FeatureProviderDataV2Spec extends Specification {

    @Shared
    ObjectMapper objectMapper = YamlSerialization.createYamlMapper()

    //TODO: works in IntelliJ, fails with "Strings too large to calculate edit distance." in gradle
    @Ignore
    def 'serialize'() {
        given:

        FeatureProviderDataV2 data = new ImmutableFeatureProviderCommonData.Builder()
                .id("geoval")
                .createdAt(1586271491161)
                .lastModified(1586271491161)
                .providerType("FEATURE")
                .providerSubType("SQL")
                .nativeCrs(OgcCrs.CRS84)
                .defaultLanguage("de")
                .putTypes2("observationsubject", new ImmutableFeatureSchema.Builder()
                        .sourcePath("/observationsubject")
                        .label("Untersuchungsobjekt")
                        .description("Ein Untersuchungsobjekt ist entweder eine Probe für Untersuchungen im Labor oder eine In-Situ-Messung")
                        .putProperties2("id", new ImmutableFeatureSchema.Builder()
                                .sourcePath("id")
                                .role(SchemaBase.Role.ID)
                                .label("Objekt-Id")
                                .description("Eindeutige Id des Objekts")
                                .constraints(new ImmutableSchemaConstraints.Builder()
                                        .regex("[a-zA-Z0-9_]{3,}")
                                        .build())
                        )
                        .putProperties2("type", new ImmutableFeatureSchema.Builder()
                                .sourcePath("_type")
                                .label("Art")
                                .constraints(new ImmutableSchemaConstraints.Builder()
                                        .codelist("geoval_type")
                                        .enumValues(ImmutableList.of("Borehole", "TrialPit"))
                                        .build())
                        )
                        .putProperties2("shortName", new ImmutableFeatureSchema.Builder()
                                .sourcePath("shortname")
                                .addTransformations(new ImmutablePropertyTransformation.Builder().codelist("nullValues").build())
                        )
                        .putProperties2("geomLowerPoint", new ImmutableFeatureSchema.Builder()
                                .sourcePath("geomlowerpoint")
                                .type(SchemaBase.Type.GEOMETRY)
                                .geometryType(GeometryType.POINT)
                        )
                        .putProperties2("explorationSite", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[explorationsite_fk=id]explorationsite")
                                .type(SchemaBase.Type.OBJECT)
                                .objectType("Link")
                                .label("Aufschlußpunkt")
                                .putProperties2("title", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("shortname")
                                )
                                .putProperties2("href", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("id")
                                        .constraints(new ImmutableSchemaConstraints.Builder()
                                                .required(true)
                                                .build()))
                        )
                        .putProperties2("process", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[id=observationsubject_fk]observationsubject_process/[process_fk=id]process")
                                .type(SchemaBase.Type.OBJECT_ARRAY)
                                .objectType("Link")
                                .label("Versuche")
                                .constraints(new ImmutableSchemaConstraints.Builder()
                                        .minOccurrence(1)
                                        .maxOccurrence(3)
                                        .build())
                                .putProperties2("title", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("category_fk")
                                )
                                .putProperties2("href", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("id")
                                        .constraints(new ImmutableSchemaConstraints.Builder()
                                                .required(true)
                                                .build())
                                )
                        )
                        .putProperties2("filterValues", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[id=observationsubject_fk]observationsubject_filtervalues")
                                .type(SchemaBase.Type.OBJECT_ARRAY)
                                .objectType("FilterValue")
                                .label("Leitkennwerte")
                                .putProperties2("property", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("filtervalueproperty_fk")
                                        .label("Eigenschaft")
                                )
                                .putProperties2("property", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("filtervalueproperty_fk")
                                        .label("Eigenschaft")
                                )
                                .putProperties2("measure", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("resultmeasure")
                                        .type(SchemaBase.Type.FLOAT)
                                        .label("Messwert")
                                        .constraints(new ImmutableSchemaConstraints.Builder()
                                                .min(0.0)
                                                .max(100.0)
                                                .build())
                                )
                                .putProperties2("classification", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("[id=filtervalue_fk]filtervalue_resultclassification/classificationcode_fk")
                                        .type(SchemaBase.Type.VALUE_ARRAY)
                                        .valueType(SchemaBase.Type.STRING)
                                        .label("Klassifikationen")
                                )
                        )
                )
                .putCodelists("nullValues", ImmutableMap.of("No Information", "null", "-1", "null"))
                .build()
        when:

        def dataAsYaml = objectMapper.writeValueAsString(data)

        then:

        def expected = new File("src/test/resources/geoval.yml").text

        dataAsYaml == expected

    }

    def "Properties are not empty when Type is not OBJECT or OBJECT_ARRAY"() {
        when:
        def property = new ImmutableFeaturePropertyV2.Builder()
                .path("testPath")
                .name("testName")
                .type(FeaturePropertyV2.Type.VALUE_ARRAY)
                .putProperties2("testProp", new ImmutableFeaturePropertyV2.Builder()
                        .path("innerPropertyPath"))
                .build()

        then:
        thrown(IllegalStateException)
    }

    def "ObjectType is set, Type is not OBJECT, OBJECT_ARRAY or GEOMETRY"() {
        when:
        def property = new ImmutableFeaturePropertyV2.Builder()
                .path("geoPath")
                .name("geoName")
                .type(FeaturePropertyV2.Type.STRING)
                .objectType("Link")
                .build()

        then:
        thrown(IllegalStateException)
    }

    def "ValueType is set, Type is OBJECT"() {
        when:
        def property = new ImmutableFeaturePropertyV2.Builder()
                .path("geoPath")
                .name("geoName")
                .type(FeaturePropertyV2.Type.OBJECT)
                .valueType(FeaturePropertyV2.Type.STRING)
                .build()

        then:
        thrown(IllegalStateException)
    }

    def "Type is VALUE_ARRAY, ValueType is not set"() {
        when:
        def property = new ImmutableFeaturePropertyV2.Builder()
                .path("geoPath")
                .name("geoName")
                .type(FeaturePropertyV2.Type.VALUE_ARRAY)
                .build()

        then:
        thrown(IllegalStateException)
    }

    def "GeometryType is present when Type is not GEOMETRY"() {
        when:
        def property = new ImmutableFeaturePropertyV2.Builder()
                .path("geoPath")
                .name("geoName")
                .type(FeaturePropertyV2.Type.STRING)
                .geometryType(GeometryType.POINT)
                .build()

        then:
        thrown(IllegalStateException)
    }

    def "Type is GEOMETRY, GeometryType is not set"() {
        when:
        def property = new ImmutableFeaturePropertyV2.Builder()
                .path("geoPath")
                .name("geoName")
                .type(FeaturePropertyV2.Type.GEOMETRY)
                .build()

        then:
        thrown(IllegalStateException)
    }
}
