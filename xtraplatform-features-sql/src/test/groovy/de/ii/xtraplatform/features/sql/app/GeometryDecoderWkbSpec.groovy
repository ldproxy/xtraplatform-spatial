package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.domain.FeatureEventHandler
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.SchemaBase.Type
import de.ii.xtraplatform.features.domain.SchemaMapping
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry
import spock.lang.Specification
import java.nio.ByteBuffer
import java.nio.ByteOrder

class GeometryDecoderWkbSpec extends Specification {

    FeatureEventHandler<FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>> handler
    ModifiableContext<FeatureSchema, SchemaMapping> context
    GeometryDecoderWkb decoder

    def setup() {
        handler = Mock(FeatureEventHandler)
        context = Mock(ModifiableContext)
        decoder = new GeometryDecoderWkb(handler, context)
    }

    def "test decode POINT"() {
        given:
        byte[] wkb = createWkbPoint(30.0, 10.0)
        String expectedToken = "30.000 10.000"

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.POINT)
        1 * context.setGeometryDimension(2)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken)
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode LINESTRING"() {
        given:
        byte[] wkb = createWkbLineString([[30.0, 10.0], [10.0, 30.0], [40.0, 40.0]])
        String expectedToken = " 30.000 10.000,10.000 30.000,40.000 40.000"

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.LINE_STRING)
        1 * context.setGeometryDimension(2)
        1 * context.setValueType(Type.STRING)
        1 * context.setValue(expectedToken) // Ensure no leading space
        1 * handler.onValue(context)
        1 * handler.onObjectEnd(context)
    }

    def "test decode POLYGON"() {
        given:
        byte[] wkb = createWkbPolygon([
                [[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]
        ])
        String expectedToken = " 30.000 10.000,40.000 40.000,20.000 40.000,10.000 20.000,30.000 10.000"

        when:
        decoder.decode(wkb)

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
        byte[] wkb = createWkbMultiPoint([[10.0, 40.0], [40.0, 30.0], [20.0, 20.0], [30.0, 10.0]])
        List<String> expectedTokens = ["10.000 40.000", "40.000 30.000", "20.000 20.000", "30.000 10.000"]

        when:
        decoder.decode(wkb)

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

    def "test decode MULTILINESTRING"() {
        given:
        byte[] wkb = createWkbMultiLineString([
                [[10.0, 10.0], [20.0, 20.0]],
                [[15.0, 15.0], [30.0, 15.0]]
        ])
        List<String> expectedTokens = [" 10.000 10.000,20.000 20.000", " 15.000 15.000,30.000 15.000"]

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_LINE_STRING)
        1 * context.setGeometryDimension(2)
        3 * handler.onArrayStart(context) // Start of MULTILINESTRING array
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        3 * handler.onArrayEnd(context) // End of MULTILINESTRING array
        1 * handler.onObjectEnd(context)
    }

    def "test decode MULTIPOLYGON"() {
        given:
        byte[] wkb = createWkbMultiPolygon([
                [[[30.0, 20.0], [45.0, 40.0], [10.0, 40.0], [30.0, 20.0]]],
                [[[15.0, 5.0], [40.0, 10.0], [10.0, 20.0], [5.0, 10.0], [15.0, 5.0]]]
        ])
        List<String> expectedTokens = [
                " 30.000 20.000,45.000 40.000,10.000 40.000,30.000 20.000",
                " 15.000 5.000,40.000 10.000,10.000 20.000,5.000 10.000,15.000 5.000"
        ]

        when:
        decoder.decode(wkb)

        then:
        1 * handler.onObjectStart(context)
        1 * context.setGeometryType(SimpleFeatureGeometry.MULTI_POLYGON)
        1 * context.setGeometryDimension(2)
        1 * handler.onArrayStart(context) // Start of MULTIPOLYGON array
        2 * handler.onArrayStart(context) // Start of each POLYGON array
        expectedTokens.each { token ->
            1 * context.setValueType(Type.STRING)
            1 * context.setValue(token)
            1 * handler.onValue(context)
        }
        2 * handler.onArrayEnd(context) // End of each POLYGON array
        1 * handler.onArrayEnd(context) // End of MULTIPOLYGON array
        1 * handler.onObjectEnd(context)
    }

    private byte[] createWkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put((byte) 1) // Little Endian
        buffer.putInt(1) // WKB Type: POINT
        buffer.putDouble(x)
        buffer.putDouble(y)
        return buffer.array()
    }

    private byte[] createWkbLineString(List<List<Double>> points) {
        ByteBuffer buffer = ByteBuffer.allocate(9 + points.size() * 16).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put((byte) 1) // Little Endian
        buffer.putInt(2) // WKB Type: LINESTRING
        buffer.putInt(points.size()) // Number of points
        points.each { p ->
            buffer.putDouble(p[0])
            buffer.putDouble(p[1])
        }
        return buffer.array()
    }
    private byte[] createWkbPolygon(List<List<List<Double>>> rings) {
        int numPoints = rings.flatten().size()
        ByteBuffer buffer = ByteBuffer.allocate(9 + (numPoints * 16)).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put((byte) 1) // Little Endian
        buffer.putInt(3) // WKB Type: POLYGON
        buffer.putInt(rings.size()) // Anzahl der Ringe
        rings.each { ring ->
            buffer.putInt(ring.size()) // Anzahl der Punkte im Ring
            ring.each { p ->
                buffer.putDouble(p[0])
                buffer.putDouble(p[1])
            }
        }
        return buffer.array()
    }

    private byte[] createWkbMultiPoint(List<List<Double>> points) {
        ByteBuffer buffer = ByteBuffer.allocate(9 + points.size() * 21).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put((byte) 1) // Little Endian
        buffer.putInt(4) // WKB Type: MULTIPOINT
        buffer.putInt(points.size()) // Anzahl der Punkte
        points.each { p ->
            buffer.put(createWkbPoint(p[0], p[1]))
        }
        return buffer.array()
    }

    private byte[] createWkbMultiLineString(List<List<List<Double>>> lineStrings) {
        int numLineStrings = lineStrings.size()
        int numPoints = lineStrings.flatten().size()
        ByteBuffer buffer = ByteBuffer.allocate(9 + (numLineStrings * 9) + (numPoints * 16)).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put((byte) 1) // Little Endian
        buffer.putInt(5) // WKB Type: MULTILINESTRING
        buffer.putInt(numLineStrings) // Number of line strings
        lineStrings.each { line ->
            buffer.put((byte) 1) // Little Endian for each line string
            buffer.putInt(2) // WKB Type: LINESTRING
            buffer.putInt(line.size()) // Number of points in the line string
            line.each { p ->
                buffer.putDouble(p[0])
                buffer.putDouble(p[1])
            }
        }
        return buffer.array()
    }

    private byte[] createWkbMultiPolygon(List<List<List<List<Double>>>> polygons) {
        int numPolygons = polygons.size()
        int numPoints = polygons.flatten().flatten().size()
        ByteBuffer buffer = ByteBuffer.allocate(9 + (numPolygons * 9) + (numPoints * 16)).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put((byte) 1) // Little Endian
        buffer.putInt(6) // WKB Type: MULTIPOLYGON
        buffer.putInt(numPolygons) // Number of polygons
        polygons.each { poly ->
            buffer.put((byte) 1) // Little Endian for each polygon
            buffer.putInt(3) // WKB Type: POLYGON
            buffer.putInt(poly.size()) // Number of rings in the polygon
            poly.each { ring ->
                buffer.putInt(ring.size()) // Number of points in the ring
                ring.each { p ->
                    buffer.putDouble(p[0])
                    buffer.putDouble(p[1])
                }
            }
        }
        return buffer.array()
    }
}