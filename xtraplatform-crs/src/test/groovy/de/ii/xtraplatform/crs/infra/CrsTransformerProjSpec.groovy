/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.infra

import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.blobs.domain.ResourceStore
import de.ii.xtraplatform.crs.domain.*
import de.ii.xtraplatform.proj.domain.ProjLoaderImpl
import org.kortforsyningen.proj.Units
import org.opengis.referencing.cs.AxisDirection
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

class CrsTransformerProjSpec extends Specification {

    @Shared
    CrsTransformerFactoryProj transformerFactory

    //NOTE: if tests fail after a proj upgrade, you might copy the previous proj data directory
    // to the location below to find out if the failures result from changes in the code or the data
    def setupSpec() {
        ResourceStore resourceStore = Stub()
        VolatileRegistry volatileRegistry = Stub()
        volatileRegistry.onAvailable(*_) >> CompletableFuture.completedFuture(null)
        transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)
        transformerFactory.onStart(false).toCompletableFuture().join()
    }

    def 'find transformer - #dim (#src, #trgt)'() {

        when:

        def transformer = transformerFactory.getTransformer(sourceCrs, targetCrs)

        then:

        transformer.isPresent()

        where:

        dim        | src          | trgt         | sourceCrs         | targetCrs
        "2d -> 2d" | "EPSG:25832" | "CRS84"      | EpsgCrs.of(25832) | OgcCrs.CRS84
        "2d -> 2d" | "CRS84"      | "EPSG:25832" | OgcCrs.CRS84      | EpsgCrs.of(25832)
        "3d -> 2d" | "EPSG:5555"  | "CRS84"      | EpsgCrs.of(5555)  | OgcCrs.CRS84
        "3d -> 2d" | "CRS84h"     | "EPSG:25832" | OgcCrs.CRS84h     | EpsgCrs.of(25832)
        "2d -> 3d" | "EPSG:25832" | "CRS84h"     | EpsgCrs.of(25832) | OgcCrs.CRS84h
        "2d -> 3d" | "CRS84"      | "EPSG:5555"  | OgcCrs.CRS84      | EpsgCrs.of(5555)
        "3d -> 3d" | "EPSG:5555"  | "CRS84h"     | EpsgCrs.of(5555)  | OgcCrs.CRS84h
        "3d -> 3d" | "CRS84h"     | "EPSG:5555"  | OgcCrs.CRS84h     | EpsgCrs.of(5555)

    }

    def 'find transformer - #sourceOrTarget CRS is null'() {
        when:
        def transformer = transformerFactory.getTransformer(sourceCrs, targetCrs)

        then:
        thrown(IllegalArgumentException)

        where:
        sourceOrTarget | sourceCrs        | targetCrs
        "target"       | EpsgCrs.of(5555) | null
        "source"       | null             | EpsgCrs.of(5555)

    }


    def 'CRS transformer test 3D'() {
        given:
        EpsgCrs sourceCrs = EpsgCrs.of(5555)
        EpsgCrs targetCrs = EpsgCrs.of(4979)
        double[] source = [420735.071, 5392914.343, 131.96]

        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] target = gct.transform(source, 1, 3)

        then:
        compareGeographicCoordinate(target, [48.68423644912392, 7.923077973066287, 131.96] as double[])
    }

    def 'CRS transformer test 2D'() {
        given:
        EpsgCrs sourceCrs = EpsgCrs.of(4326)
        EpsgCrs targetCrs = EpsgCrs.of(3857)
        double x = 50.7164
        double y = 7.086
        double rx = 788809.9117611365
        double ry = 6571280.658009369
        double[] ra = new double[10]
        for (int i = 0; i < 10; i += 2) {
            ra[i] = x
            ra[i + 1] = y
        }

        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        CoordinateTuple coordinateTuple1 = gct.transform(x, y)
        CoordinateTuple coordinateTuple2 = gct.transform(new CoordinateTuple(x, y))
        double[] re = gct.transform(ra, 5, 2)

        then:
        compareValueInMeter(coordinateTuple1.getX(), rx)
        compareValueInMeter(coordinateTuple1.getY(), ry)
        compareValueInMeter(coordinateTuple2.getX(), rx)
        compareValueInMeter(coordinateTuple2.getY(), ry)
        for (int i = 0; i < 10; i += 2) {
            compareValueInMeter(re[i], rx)
            compareValueInMeter(re[i + 1], ry)
        }
    }

    def 'CRS transformer test 3D to 2D - Geographic CRS'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] result = gct.transform(source as double[], 1, 3)

        then:
        compareGeographicCoordinate(result, target as double[])

        where:
        sourceCrs        | targetCrs         | source                            | target
        EpsgCrs.of(5555) | EpsgCrs.of(4326)  | [420735.071, 5392914.343, 131.96] | [48.68423644912392, 7.923077973066287, 0.0]
        EpsgCrs.of(5555) | OgcCrs.CRS84      | [420735.071, 5392914.343, 131.96] | [7.923077973066287, 48.68423644912392, 0.0]

        EpsgCrs.of(4979) | EpsgCrs.of(4326)  | [48.684, 7.923, 131.96]           | [48.684, 7.923, 0.0]
        EpsgCrs.of(4979) | OgcCrs.CRS84      | [48.684, 7.923, 131.96]           | [7.923, 48.684, 0.0]

        OgcCrs.CRS84h    | EpsgCrs.of(4326)  | [7.923, 48.684, 131.96]           | [48.684, 7.923, 0.0]
        OgcCrs.CRS84h    | OgcCrs.CRS84      | [7.923, 48.684, 131.96]           | [7.923, 48.684, 0.0]

    }

    def 'CRS transformer test 3D to 2D - Projected CRS'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] result = gct.transform(source as double[], 1, 3)

        then:
        compareValuesInMeter(result, target as double[])

        where:
        sourceCrs        | targetCrs         | source                            | target
        EpsgCrs.of(5555) | EpsgCrs.of(25832) | [420735.071, 5392914.343, 131.96] | [420735.071, 5392914.343, 0.0]
        EpsgCrs.of(5555) | EpsgCrs.of(3857)  | [420735.071, 5392914.343, 131.96] | [881993.0054771411, 6221451.78116173, 0.0]

        EpsgCrs.of(4979) | EpsgCrs.of(25832) | [48.684, 7.923, 131.96]           | [420728.9609056481, 5392888.1411416, 0.0]
        EpsgCrs.of(4979) | EpsgCrs.of(3857)  | [48.684, 7.923, 131.96]           | [881984.3255551065, 6221411.912936983, 0.0]

        OgcCrs.CRS84h    | EpsgCrs.of(25832) | [7.923, 48.684, 131.96]           | [420728.9609056481, 5392888.1411416, 0.0]
        OgcCrs.CRS84h    | EpsgCrs.of(3857)  | [7.923, 48.684, 131.96]           | [881984.3255551065, 6221411.912936983, 0.0]

    }

    def 'CRS transformer - 2D to a geocentric CRS'() {
        given:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(EpsgCrs.of(25832), EpsgCrs.of(4936)).get()

        when:
        double[] result = gct.transform([500000.0, 5550000.0] as double[], 1, 2)

        then:
        gct.getTargetDimension() == 3
        result.length == 3
        Math.abs(result[0] - 4046000) < 10000
        Math.abs(result[1] - 641000) < 10000
        Math.abs(result[2] - 4869000) < 10000
    }

    def 'CRS transformer - dimension mismatch'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(EpsgCrs.of(25832), EpsgCrs.of(5555)).get()
        double[] result = gct.transform([420735.071, 5392914.343, 131.96] as double[], 1, 3)

        then:
        thrown(IllegalStateException)
    }

    def 'CRS transformer - methods'() {
        when:
        def transformer = transformerFactory.getTransformer(EpsgCrs.of(5555), EpsgCrs.of(25832)).get()

        then:
        transformer.getSourceCrs() == EpsgCrs.of(5555)
        transformer.getSourceDimension() == 3
        transformer.getSourceUnitEquivalentInMeters() == 1.0
        transformer.getTargetCrs() == EpsgCrs.of(25832)
        transformer.getTargetDimension() == 2
        transformer.getTargetUnitEquivalentInMeters() == 1.0

    }

    def 'CRS info test'() {
        expect:
        transformerFactory.isSupported(crs) == true
        transformerFactory.getUnit(crs) == unit
        transformerFactory.getAxisAbbreviations(crs) == axisAbbreviations
        transformerFactory.getAxisUnits(crs) == axisUnits
        transformerFactory.getAxisDirections(crs) == axisDirections
        transformerFactory.getAxisWithWraparound(crs) == axisWithWraparound
        transformerFactory.getAxisMinimums(crs) == axisMinimums
        transformerFactory.getAxisMaximums(crs) == axisMaximums
        transformerFactory.getDomainOfValidity(crs) == domainOfValidity

        where:
        crs               | unit | axisAbbreviations   | axisUnits                                 | axisDirections                                              | axisWithWraparound  | axisMinimums                                                                                                          | axisMaximums                                                                                                          | domainOfValidity
        EpsgCrs.of(5555)  | Units.METRE | ["E", "N", "H"]     | [Units.METRE, Units.METRE, Units.METRE]   | [AxisDirection.EAST, AxisDirection.NORTH, AxisDirection.UP] | OptionalInt.empty() | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)] | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)] | Optional.of(BoundingBox.of(6.0, 47.27, 12.0, 55.09, OgcCrs.CRS84))
        EpsgCrs.of(5556)  | Units.METRE | ["E", "N", "H"]     | [Units.METRE, Units.METRE, Units.METRE]   | [AxisDirection.EAST, AxisDirection.NORTH, AxisDirection.UP] | OptionalInt.empty() | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)] | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)] | Optional.of(BoundingBox.of(12.0, 47.46, 15.04, 54.74, OgcCrs.CRS84))
        EpsgCrs.of(4979)  | Units.DEGREE | ["Lat", "Lon", "h"] | [Units.DEGREE, Units.DEGREE, Units.METRE] | [AxisDirection.NORTH, AxisDirection.EAST, AxisDirection.UP] | OptionalInt.of(1)   | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)] | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)] | Optional.of(BoundingBox.of(-180.0, -90.0, 180.0, 90.0, OgcCrs.CRS84))
        EpsgCrs.of(25832) | Units.METRE | ["E", "N"]          | [Units.METRE, Units.METRE]                | [AxisDirection.EAST, AxisDirection.NORTH]                   | OptionalInt.empty() | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)]                                        | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)]                                        | Optional.of(BoundingBox.of(6.0, 38.76, 12.01, 84.33, OgcCrs.CRS84))
        EpsgCrs.of(4326)  | Units.DEGREE | ["Lat", "Lon"]      | [Units.DEGREE, Units.DEGREE]              | [AxisDirection.NORTH, AxisDirection.EAST]                   | OptionalInt.of(1)   | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)]                                        | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)]                                        | Optional.of(BoundingBox.of(-180.0, -90.0, 180.0, 90.0, OgcCrs.CRS84))
        EpsgCrs.of(3857)  | Units.METRE | ["X", "Y"]          | [Units.METRE, Units.METRE]                | [AxisDirection.EAST, AxisDirection.NORTH]                   | OptionalInt.empty() | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)]                                        | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)]                                        | Optional.of(BoundingBox.of(-180.0, -85.06, 180.0, 85.06, OgcCrs.CRS84))
        EpsgCrs.of(4269)  | Units.DEGREE | ["Lat", "Lon"]      | [Units.DEGREE, Units.DEGREE]              | [AxisDirection.NORTH, AxisDirection.EAST]                   | OptionalInt.of(1)   | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)]                                        | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)]                                        | Optional.of(BoundingBox.of(167.65, 14.92, -40.73, 86.45, OgcCrs.CRS84))
        OgcCrs.CRS84      | Units.DEGREE | ["Lon", "Lat"]      | [Units.DEGREE, Units.DEGREE]              | [AxisDirection.EAST, AxisDirection.NORTH]                   | OptionalInt.of(0)   | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)]                                        | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)]                                        | Optional.of(BoundingBox.of(-180.0, -90.0, 180.0, 90.0, OgcCrs.CRS84))
        OgcCrs.CRS84h     | Units.DEGREE | ["Lon", "Lat", "h"] | [Units.DEGREE, Units.DEGREE, Units.METRE] | [AxisDirection.EAST, AxisDirection.NORTH, AxisDirection.UP] | OptionalInt.of(0)   | [Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY), Optional.of(Double.NEGATIVE_INFINITY)] | [Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY), Optional.of(Double.POSITIVE_INFINITY)] | Optional.of(BoundingBox.of(-180.0, -90.0, 180.0, 90.0, OgcCrs.CRS84))

    }

    def 'Compound CRS test - Geographic CRS'() {

        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] result = gct.transform(source as double[], 1, 3)

        then:
        compareGeographicCoordinate(result, target as double[])

        where:
        sourceCrs               | targetCrs               | source                            | target

        EpsgCrs.of(25832, 7837) | EpsgCrs.of(4979)        | [420735.071, 5392914.343, 131.96] | [48.68423644912392, 7.923077973066287, 131.96]
        EpsgCrs.of(25832, 7837) | OgcCrs.CRS84h           | [420735.071, 5392914.343, 131.96] | [7.923077973066287, 48.68423644912392, 131.96]

    }

    def 'Compound CRS test - Projected CRS'() {

        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] result = gct.transform(source as double[], 1, 3)

        then:
        compareValuesInMeter(result, target as double[])

        where:
        sourceCrs               | targetCrs               | source                            | target

        EpsgCrs.of(5555)        | EpsgCrs.of(25832, 7837) | [420735.071, 5392914.343, 131.96] | [420735.071, 5392914.343, 131.96]
        EpsgCrs.of(4979)        | EpsgCrs.of(25832, 7837) | [48.684, 7.923, 131.96]           | [420728.9609056481, 5392888.1411416, 131.96]
        OgcCrs.CRS84h           | EpsgCrs.of(25832, 7837) | [7.923, 48.684, 131.96]           | [420728.9609056481, 5392888.1411416, 131.96]

        EpsgCrs.of(25832, 7837) | EpsgCrs.of(5555)        | [420735.071, 5392914.343, 131.96] | [420735.071, 5392914.343, 131.96]

    }

    def 'CRS transformer test 2D with 3D transformer - Geographic CRS'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] result = gct.transform(source as double[], 1, 2)

        then:
        compareGeographicCoordinate(result, target as double[])

        where:
        sourceCrs               | targetCrs               | source                    | target
        EpsgCrs.of(5555)        | EpsgCrs.of(4326)        | [420735.071, 5392914.343] | [48.68423644912392, 7.923077973066287]
        EpsgCrs.of(5555)        | OgcCrs.CRS84            | [420735.071, 5392914.343] | [7.923077973066287, 48.68423644912392]

        EpsgCrs.of(4979)        | EpsgCrs.of(4326)        | [48.684, 7.923]           | [48.684, 7.923]
        EpsgCrs.of(4979)        | OgcCrs.CRS84            | [48.684, 7.923]           | [7.923, 48.684]

        OgcCrs.CRS84h           | EpsgCrs.of(4326)        | [7.923, 48.684]           | [48.684, 7.923]
        OgcCrs.CRS84h           | OgcCrs.CRS84            | [7.923, 48.684]           | [7.923, 48.684]

        EpsgCrs.of(25832, 7837) | EpsgCrs.of(4979)        | [420735.071, 5392914.343] | [48.68423644912392, 7.923077973066287]
        EpsgCrs.of(25832, 7837) | OgcCrs.CRS84h           | [420735.071, 5392914.343] | [7.923077973066287, 48.68423644912392]
    }

    def 'CRS transformer test 2D with 3D transformer - Projected CRS'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceCrs, targetCrs).get()
        double[] result = gct.transform(source as double[], 1, 2)

        then:
        compareValuesInMeter(result, target as double[])

        where:
        sourceCrs               | targetCrs               | source                    | target
        EpsgCrs.of(5555)        | EpsgCrs.of(25832)       | [420735.071, 5392914.343] | [420735.071, 5392914.343]
        EpsgCrs.of(5555)        | EpsgCrs.of(3857)        | [420735.071, 5392914.343] | [881993.0054771411, 6221451.78116173]

        EpsgCrs.of(4979)        | EpsgCrs.of(25832)       | [48.684, 7.923]           | [420728.9609056481, 5392888.1411416]
        EpsgCrs.of(4979)        | EpsgCrs.of(3857)        | [48.684, 7.923]           | [881984.3255551065, 6221411.912936983]

        OgcCrs.CRS84h           | EpsgCrs.of(25832)       | [7.923, 48.684]           | [420728.9609056481, 5392888.1411416]
        OgcCrs.CRS84h           | EpsgCrs.of(3857)        | [7.923, 48.684]           | [881984.3255551065, 6221411.912936983]

        EpsgCrs.of(5555)        | EpsgCrs.of(25832, 7837) | [420735.071, 5392914.343] | [420735.071, 5392914.343]
        EpsgCrs.of(4979)        | EpsgCrs.of(25832, 7837) | [48.684, 7.923]           | [420728.9609056481, 5392888.1411416]
        OgcCrs.CRS84h           | EpsgCrs.of(25832, 7837) | [7.923, 48.684]           | [420728.9609056481, 5392888.1411416]

        EpsgCrs.of(25832, 7837) | EpsgCrs.of(5555)        | [420735.071, 5392914.343] | [420735.071, 5392914.343]
    }

    def 'CRS transformBoundingBox - Geographic CRS'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceBbox.getEpsgCrs(), targetBbox.getEpsgCrs()).get()
        BoundingBox result = gct.transformBoundingBox(sourceBbox)

        then:
        compareValuesInDegrees(result.toArray(), targetBbox.toArray)

        where:
        sourceBbox                                                                                         | targetBbox
        BoundingBox.of(420735.071, 5392914.343, 131.96, 430735.071, 5492914.343, 141.96, EpsgCrs.of(5555)) | BoundingBox.of(48.68423644912392, 7.923077973066287, 131.96, 49.58484311245754, 8.041737428193695, 141.96, EpsgCrs.of(4979))

    }

    def 'CRS transformBoundingBox - Projected CRS'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceBbox.getEpsgCrs(), targetBbox.getEpsgCrs()).get()
        BoundingBox result = gct.transformBoundingBox(sourceBbox)

        then:
        compareValuesInMeter(result.toArray(), targetBbox.toArray)

        where:
        sourceBbox                                                                                         | targetBbox
        BoundingBox.of(420735.071, 5392914.343, 430735.071, 5492914.343, EpsgCrs.of(5555))                 | BoundingBox.of(420735.071, 5392914.343, 430735.071, 5492914.343, EpsgCrs.of(25832))

    }

    def 'CRS transformBoundingBox - exception'() {
        when:
        CrsTransformerProj gct = (CrsTransformerProj) transformerFactory.getTransformer(sourceBbox.getEpsgCrs(), targetCrs).get()
        BoundingBox result = gct.transformBoundingBox(sourceBbox)

        then:
        thrown(CrsTransformationException)

        where:
        sourceBbox                                                        | targetCrs
        BoundingBox.of(0, 100, 131.96, -1, 101, 141.96, EpsgCrs.of(4979)) | EpsgCrs.of(25832)
        BoundingBox.of(0, 100, -1, 101, EpsgCrs.of(4326))                 | EpsgCrs.of(25832)

    }

    static void compareValueInMeter(double val1, double val2) {
        // compare mm
        assert Math.abs(val1-val2) < 1e-3;
    }

    static void compareValueInDegree(double val1, double val2) {
        // compare µm
        assert Math.abs(val1-val2) < 1e-11;
    }

    static void compareGeographicCoordinate(double[] val1, double[] val2) {
        assert val1.length == val2.length
        for (int i = 0; i < val1.length; i++) {
            if (i<2)
                compareValueInDegree(val1[i],val2[i])
            else
                compareValueInMeter(val1[i],val2[i])
        }
    }

    static void compareValuesInMeter(double[] val1, double[] val2) {
        assert val1.length == val2.length
        for (int i = 0; i < val1.length; i++) {
            compareValueInMeter(val1[i],val2[i])
        }
    }

    static void compareValuesInDegrees(double[] val1, double[] val2) {
        assert val1.length == val2.length
        for (int i = 0; i < val1.length; i++) {
            compareValueInDegree(val1[i],val2[i])
        }
    }
}
