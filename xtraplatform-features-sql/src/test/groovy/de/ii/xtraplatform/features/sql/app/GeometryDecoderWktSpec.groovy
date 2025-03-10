package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.domain.FeatureEventHandler
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.SchemaBase.Type
import de.ii.xtraplatform.features.domain.SchemaMapping
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry
import spock.lang.Specification

class GeometryDecoderWktSpec extends Specification {

    FeatureEventHandler<FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>> handler
    ModifiableContext<FeatureSchema, SchemaMapping> context
    GeometryDecoderWkt decoder

    def setup() {
        handler = Mock(FeatureEventHandler)
        context = Mock(ModifiableContext)
        decoder = new GeometryDecoderWkt(handler, context)
    }

    def "test decode POINT"() {
        given:
        String wkt = "POINT (30 10)"
        String expectedToken = "30 10"

        when:
        decoder.decode(wkt)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.POINT)
        1 * context.setGeometryDimension(2)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode POLYGON"() {
        given:
        String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
        String expectedToken = "30 10,40 40,20 40,10 20,30 10"

        when:
        decoder.decode(wkt)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.POLYGON)
        1 * context.setGeometryDimension(2)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onArrayStart(context)
        1 * handler.onArrayEnd(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTIPOINT"() {
        given:
        String wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"
        List<String> expectedTokens = ["10 40", "40 30", "20 20", "30 10"]

        when:
        decoder.decode(wkt)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_POINT)
        1 * context.setGeometryDimension(2)
        1 * handler.onArrayStart(context)
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        1 * handler.onArrayEnd(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode LINESTRING"() {
        given:
        String wkt = "LINESTRING (30 10, 10 30, 40 40)"
        String expectedToken = "30 10,10 30,40 40"

        when:
        decoder.decode(wkt)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.LINE_STRING)
        1 * context.setGeometryDimension(2)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTIPOLYGON"() {
        given:
        String wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"
        List<String> expectedTokens = ["30 20,45 40,10 40,30 20", "15 5,40 10,10 20,5 10,15 5"]

        when:
        decoder.decode(wkt)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_POLYGON)
        1 * context.setGeometryDimension(2)
        3 * handler.onArrayStart(context)
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        3 * handler.onArrayEnd(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTILINESTRING"() {
        given:
        String wkt = "MULTILINESTRING ((10 10, 20 20), (15 15, 30 15))"
        List<String> expectedTokens = ["10 10,20 20", "15 15,30 15"]

        when:
        decoder.decode(wkt)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_LINE_STRING)
        1 * context.setGeometryDimension(2)
        1 * handler.onArrayStart(context)
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        1 * handler.onArrayEnd(context)
        1 * handler.onObjectEnd(context)
    }
}