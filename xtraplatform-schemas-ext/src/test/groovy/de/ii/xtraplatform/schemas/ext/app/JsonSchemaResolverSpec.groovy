package de.ii.xtraplatform.schemas.ext.app

import com.fasterxml.jackson.databind.ObjectMapper
import de.ii.xtraplatform.features.domain.*
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.schemas.ext.domain.ImmutableJsonSchemaConfiguration
import de.ii.xtraplatform.blobs.domain.ResourceStore
import spock.lang.Shared
import spock.lang.Specification

class JsonSchemaResolverSpec extends Specification {

    static FeatureProviderDataV2 data = new ImmutableFeatureProviderCommonData.Builder()
            .id("foo")
            .providerType("FEATURE")
            .providerSubType("BAR")
            .addExtensions(new ImmutableJsonSchemaConfiguration.Builder()
                    .enabled(true)
                    .geometryTypeRefs([
                            '#/$defs/polygonGeoJSON'     : GeometryType.POLYGON,
                            '#/$defs/multipolygonGeoJSON': GeometryType.MULTI_POLYGON
                    ])
                    .relationRefs(['#/$defs/Adres': 'adres'])
                    .compositionIndexes(['#/$defs/Waardetype': 5])
                    .build())
            .build()

    @Shared
    ObjectMapper objectMapper = YamlSerialization.createYamlMapper()
    @Shared
    ResourceStore schemaStore = Mock {
        has(_) >> { args -> new File("src/test/resources/schemas/${args[0]}").exists() }
        content(_) >> { args -> Optional.ofNullable(new File("src/test/resources/schemas/${args[0]}").newInputStream()) }
    }
    @Shared
    ResourceStore schemaStore2 = Mock {
        with(_) >> schemaStore
    }
    @Shared
    JsonSchemaResolver jsonSchemaResolver = new JsonSchemaResolver(schemaStore2)
    @Shared
    SchemaReferenceResolver schemaReferenceResolver = new SchemaReferenceResolver(data, () -> Set.of(jsonSchemaResolver))

    static String txt(String file) {
        return new File("src/test/resources/mappings/${file}").text
    }

    static Map<String, FeatureSchema> type(String name, String schema, Map<String, FeatureSchema> overrides) {
        return [
                (name): new Builder()
                        .name(name)
                        .schema(schema)
                        .propertyMap(overrides)
                        .build()
        ]
    }

    String resolve(Map<String, FeatureSchema> types) {
        def resolved = schemaReferenceResolver.resolve(types)

        return objectMapper.writeValueAsString(resolved)
    }

    static Map<String, FeatureSchema> gebouwOverrides = [
            "geregistreerdMet": new Builder()
                    .name("geregistreerdMet")
                    .propertyMap([
                            "bestaatUit": new Builder()
                                    .name("bestaatUit")
                                    .propertyMap([
                                            "waarde": new Builder()
                                                    .name("waarde")
                                                    .sourcePath("waarde/stringValue")
                                                    .build()
                                    ])
                                    .build()
                    ])
                    .build()
    ]

    def 'resolve'() {
        expect:

        resolve(type(type, schema, overrides)) == txt(expected)

        where:

        type     | schema                               | overrides       || expected
        "link"   | "link.json"                          | [:]             || "link.yml"
        //"gebouw" | "gebouw.json"                        | gebouwOverrides || "gebouw.yml"
        "xplan"  | 'xplan.json#/$defs/PFS_Schutzgebiet' | [:]             || "xplan.yml"
    }
}
