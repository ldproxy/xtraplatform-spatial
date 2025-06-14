package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.transform.FeatureEventBuffer
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations
import de.ii.xtraplatform.features.domain.transform.TokenSliceTransformerChain

import java.time.ZoneId
import java.util.function.Function

class Util {

    static FeatureTokenReader createReader(FeatureSchema schema, List<Object> target, Map<String, List<PropertyTransformation>> transformations = Map.of()) {
        FeatureTokenTransformerMappings mapper = createMapper(target, transformations)

        FeatureEventHandler.ModifiableContext context = createContext(mapper, schema)

        return createReader(mapper, context)
    }

    static FeatureTokenReader createReader(FeatureTokenTransformerMappings mapper, FeatureEventHandler.ModifiableContext context) {
        return new FeatureTokenReader(mapper, context)
    }

    static FeatureEventHandler.ModifiableContext createContext(FeatureTokenTransformerMappings mapper, FeatureSchema schema, boolean useTargetPaths = false) {
        FeatureQuery query = ImmutableFeatureQuery.builder().type("test").build()
        FeatureEventHandler.ModifiableContext context = mapper.createContext()
                .setQuery(query)
                .setMappings([test: SchemaMapping.of(schema)])
                .setType('test')
                .setIsUseTargetPaths(useTargetPaths)

        return context
    }

    static FeatureTokenTransformerMappings createMapper(List<Object> target, Map<String, List<PropertyTransformation>> transformations = Map.of()) {
        FeatureTokenTransformerMappings mapper = new FeatureTokenTransformerMappings(["test": new PropertyTransformations() {
            @Override
            Map<String, List<PropertyTransformation>> getTransformations() {
                return transformations;
            }
        }], Map.of(), ZoneId.of("UTC"))

        mapper.init(token -> target.add(token))

        return mapper
    }

    static TokenSliceTransformerChain createSliceTransformerChain(FeatureSchema schema, Map<String, List<PropertyTransformation>> transformations = Map.of()) {
        return new TokenSliceTransformerChain(transformations, SchemaMapping.of(schema), Function.identity(), Map.of(), ZoneId.of("UTC"))
    }

    static FeatureEventBuffer<
            FeatureSchema, SchemaMapping, FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>> createBuffer(FeatureSchema schema, List<Object> target, List<Object> source = []) {
        FeatureTokenTransformerMappings mapper = createMapper([])

        FeatureEventHandler.ModifiableContext context = createContext(mapper, schema)

        FeatureTokenEmitter2<FeatureSchema, SchemaMapping, FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>> downstream = token -> target.add(token)

        FeatureEventBuffer<
                FeatureSchema, SchemaMapping, FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>> buffer = new FeatureEventBuffer<>(downstream, context, context.mappings())

        def mapping = context.mapping()

        for (Object token : source) {
            mapping.getPositionsForTargetPath()
        }

        return buffer
    }

}
