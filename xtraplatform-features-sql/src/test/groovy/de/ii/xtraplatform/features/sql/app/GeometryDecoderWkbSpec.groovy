package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.domain.FeatureEventHandler
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext
import de.ii.xtraplatform.features.domain.SchemaBase.Type
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKBWriter
import spock.lang.Specification

class GeometryDecoderWkbSpec extends Specification {

    GeometryFactory geometryFactory = new GeometryFactory();
    def handler = Mock(FeatureEventHandler)
    def context = Mock(ModifiableContext)
    def decoder = new GeometryDecoderWkb(handler, context)
    def wkbWriter = new WKBWriter()

    def "test decode POINT"() {
        given:
        byte[] wkb = wkbWriter.write(geometryFactory.createPoint(new Coordinate(30, 10)))
        String expectedToken = "POINT (30 10)"

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.POINT)
        1 * context.setGeometryDimension(3)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode LINESTRING"() {
        given:
        byte[] wkb = wkbWriter.write(geometryFactory.createLineString([
                new Coordinate(10, 10),
                new Coordinate(20, 20),
                new Coordinate(30, 40)
        ] as Coordinate[]))
        String expectedToken = "LINESTRING (10 10, 20 20, 30 40)"

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.LINE_STRING)
        1 * context.setGeometryDimension(3)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode POLYGON"() {
        given:
        byte[] wkb = wkbWriter.write(geometryFactory.createPolygon([
                new Coordinate(30, 10),
                new Coordinate(40, 40),
                new Coordinate(20, 40),
                new Coordinate(10, 20),
                new Coordinate(30, 10)
        ] as Coordinate[]))
        String expectedToken = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.POLYGON)
        1 * context.setGeometryDimension(3)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTIPOINT"() {
        given:
        byte[] wkb = wkbWriter.write(geometryFactory.createMultiPoint([
                geometryFactory.createPoint(new Coordinate(10, 40)),
                geometryFactory.createPoint(new Coordinate(40, 30)),
                geometryFactory.createPoint(new Coordinate(20, 20)),
                geometryFactory.createPoint(new Coordinate(30, 10))
        ] as Point[]))
        List<String> expectedTokens = ["POINT (10 40)", "POINT (40 30)", "POINT (20 20)", "POINT (30 10)"]

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_POINT)
        1 * context.setGeometryDimension(3)
        1 * handler.onArrayStart(context)
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        1 * handler.onArrayEnd(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTILINESTRING"() {
        given:
        byte[] wkb = wkbWriter.write(geometryFactory.createMultiLineString([
                geometryFactory.createLineString([new Coordinate(10, 10), new Coordinate(20, 20)] as Coordinate[]),
                geometryFactory.createLineString([new Coordinate(30, 30), new Coordinate(40, 40)] as Coordinate[])
        ] as LineString[]))
        List<String> expectedTokens = ["LINESTRING (10 10, 20 20)", "LINESTRING (30 30, 40 40)"]

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_LINE_STRING)
        1 * context.setGeometryDimension(3)
        1 * handler.onArrayStart(context)
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        1 * handler.onArrayEnd(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTIPOLYGON"() {
        given:
        byte[] wkb = wkbWriter.write(geometryFactory.createMultiPolygon([
                geometryFactory.createPolygon([
                        new Coordinate(10, 10),
                        new Coordinate(20, 20),
                        new Coordinate(20, 10),
                        new Coordinate(10, 10)
                ] as Coordinate[]),
                geometryFactory.createPolygon([
                        new Coordinate(30, 30),
                        new Coordinate(40, 40),
                        new Coordinate(40, 30),
                        new Coordinate(30, 30)
                ] as Coordinate[])
        ] as Polygon[]))
        List<String> expectedTokens = [
                "POLYGON ((10 10, 20 20, 20 10, 10 10))",
                "POLYGON ((30 30, 40 40, 40 30, 30 30))"
        ]

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_POLYGON)
        1 * context.setGeometryDimension(3)
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