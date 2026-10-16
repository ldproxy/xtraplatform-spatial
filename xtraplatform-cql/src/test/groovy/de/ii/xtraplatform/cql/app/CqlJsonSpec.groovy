/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.cql.domain.BinarySpatialOperation
import de.ii.xtraplatform.cql.domain.GeometryNode
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.crs.domain.OgcCrs
import org.skyscreamer.jsonassert.JSONAssert
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

class CqlJsonSpec extends Specification {

    @Shared
    Cql cql

    def setupSpec() {
        cql = new CqlImpl()
    }

    def 'Floors greater than 5'() {

        given:
        String cqlJson = """
            {
                "op": ">",
                "args": [ { "property": "floors" }, 5 ]
            } 
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_1

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_1, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Taxes less than or equal to 500'() {

        given:
        String cqlJson = """
            {
                 "op": "<=",
                 "args": [ { "property": "taxes" }, 500 ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_2

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_2, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Owner name contains "Jones"'() {

        given:
        String cqlJson = """
        {
            "op": "like",
            "args": [ { "property": "owner" }, "% Jones %" ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_3

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_3, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Owner name starts with "Mike"'() {

        given:
        String cqlJson = """
        {
            "op": "like",
            "args": [ { "property": "owner" }, "Mike%" ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_4

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_4, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Owner name does not contain "Mike"'() {

        given:
        String cqlJson = """
        {
            "op": "not",
            "args": [ 
                { 
                    "op": "like", 
                    "args": [ { "property": "owner" }, "% Mike %" ] 
                } 
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_5

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_5, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'A swimming pool'() {

        given:
        String cqlJson = """
        {
            "op": "=",
            "args": [ { "property": "swimming_pool" }, true ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_6

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_6, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'More than 5 floors and a swimming pool'() {

        given:
        String cqlJson = """
            {
                "op": "and",
                "args": [
                    {
                        "op": ">",
                        "args": [ { "property": "floors" }, 5 ]
                    },
                    {
                        "op": "=",
                        "args": [ { "property": "swimming_pool" }, true ]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_7

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_7, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'A swimming pool and (more than five floors or material is brick)'() {

        given:
        String cqlJson = """
        {
            "op": "and",
            "args": [
                {
                    "op": "=",
                    "args": [ { "property": "swimming_pool" }, true ]
                },
                {
                    "op": "or",
                    "args": [
                        {
                            "op": ">",
                            "args": [ { "property": "floors" }, 5 ]
                        },
                        {
                            "op": "like",
                            "args": [ { "property": "material" }, "brick%" ]
                        },
                        {
                            "op": "like",
                            "args": [ { "property": "material" }, "%brick" ]
                        }                        
                    ]
                }
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_8

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_8, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def '[More than five floors and material is brick] or swimming pool is true'() {

        given:
        String cqlJson = """
        {
            "op": "or",
            "args": [
                {
                    "op": "and",
                    "args": [
                        {
                            "op": ">",
                            "args": [
                                {"property": "floors"},
                                5
                            ]
                        },
                        {
                            "op": "=",
                            "args": [
                                {"property": "material"},
                                "brick"
                            ]
                        }
                    ]
                },
                {
                    "op": "=",
                    "args": [
                        { "property": "swimming_pool" },
                        true
                    ]
                }
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_9

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_9, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Not under 5 floors or a swimming pool'() {

        given:
        String cqlJson = """
        {
            "op": "or",
            "args": [
                {
                    "op": "not",
                    "args": [
                        {
                            "op": "<",
                            "args": [
                                {"property": "floors"},
                                5
                            ]
                        }         
                    ]          
                },
                {
                    "op": "=",
                    "args": [
                        {"property": "swimming_pool"},
                        true
                    ]
                }
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_10

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_10, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Owner name starts with "mike" or "Mike" and is less than 4 floors'() {

        given:
        String cqlJson = """
        {
            "op": "and",
            "args": [
                {
                    "op": "or",
                    "args": [
                        {
                            "op": "like",
                            "args": [
                                {"property": "owner"},
                                "mike%"
                            ]
                        },
                        {
                            "op": "like",
                            "args": [
                                {"property": "owner"},
                                "Mike%"
                            ]
                        }
                    ]
                },
                {
                    "op": "<",
                    "args": [
                        {"property": "floors"},
                        4
                    ]
                }
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_11

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_11, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Built before June 5, 2012'() {

        given:
        String cqlJson = """
        {
            "op": "t_before",
            "args": [
                {"property": "built"},
                {"timestamp": "2012-06-05T00:00:00Z"}
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_12

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_12, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Built after June 5, 2012'() {

        given:
        String cqlJson = """
        {
            "op": "t_after",
            "args": [
                {"property": "built"},
                {"timestamp": "2012-06-05T00:00:00Z"}
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_13

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_13, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Updated between 7:30am June 10, 2017 and 10:30am June 11, 2017'() {

        given:
        String cqlJson = """
            {
                "op": "t_during",
                "args": [
                    {"property": "updated"},
                    {"interval": ["2017-06-10T07:30:00Z","2017-06-11T10:30:00Z"]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_14

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_14, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location in the box between -118,33.8 and -117.9,34 in lat/long (geometry 1)'() {

        given:
        String cqlJson = """
        {
            "op": "s_within",
            "args": [
                {"property": "location"},
                { "bbox": [-118,33.8,-117.9,34] }
             ] 
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_15

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_15, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Location that intersects with a point geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "Point",
                        "coordinates": [10.0, -10.0]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_Point

        and:

        when: 'writing json'
        String actual2 = cql.write(actual, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a line string geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "LineString",
                        "coordinates": [[-10.0, -10.0],[10.0, -10.0],[10.0, 10.0],[-10.0, -10.0]]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_LineString

        and:

        when: 'writing json'
        String actual2 = cql.write(actual, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a polygon geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "Polygon",
                        "coordinates": [[[-10.0, -10.0],[10.0, -10.0],[10.0, 10.0],[-10.0, -10.0]]]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16

        and:

        when: 'writing json'
        String actual2 = cql.write(actual, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a multi-point geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "MultiPoint",
                        "coordinates": [[10.0, -10.0],[10.0, 10.0]]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_MultiPoint

        and:

        when: 'writing json'
        String actual2 = cql.write(actual, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a multi-line-string geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "MultiLineString",
                        "coordinates": [[[-10.0, -10.0],[10.0, -10.0],[10.0, 10.0],[-10.0, -10.0]], [[-15.0, -15.0],[15.0, -15.0],[15.0, 15.0],[-15.0, -15.0]]]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_MultiLineString

        and:

        when: 'writing json'
        String actual2 = cql.write(actual, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a multi-polygon geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "MultiPolygon",
                        "coordinates": [[[[-10.0, -10.0],[10.0, -10.0],[10.0, 10.0],[-10.0, -10.0]]], [[[-15.0, -15.0],[15.0, -15.0],[15.0, 15.0],[-15.0, -15.0]]]]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_MultiPolygon

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_16_MultiPolygon, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a bbox geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    { "bbox": [-10.0, -10.0, 10.0, 10.0] }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_BBox

        and:

        when: 'writing json'
        String actual2 = cql.write(actual, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Location that intersects with a geometry collection geometry'() {

        given:
        String cqlJson = """
            {
                "op": "s_intersects",
                "args": [
                    {"property": "location"},
                    {
                        "type": "GeometryCollection",
                        "geometries": [
                            {
                                "type": "Point",
                                "coordinates": [10.0, -10.0]
                            },
                            {
                                "type": "Point",
                                "coordinates": [10.0, 10.0]
                            }
                        ]
                    }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_16_GeometryCollection

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_16_GeometryCollection, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'More than 5 floors and is within geometry 1 (below)'() {

        given:
        String cqlJson = """
        {
            "op": "and",
            "args": [
                {
                    "op": ">",
                    "args": [
                        {"property": "floors"},
                        5
                    ]
                },
                {
                   "op": "s_within",
                    "args": [
                      {"property": "geometry"},
                      { 
                        "bbox": [-118,33.8,-117.9,34]
                      }
                   ]
                }
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_17

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_17, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Number of floors between 4 and 8'() {

        given:
        String cqlJson = """
        {
            "op": "between",
            "args": [
                {"property": "floors"},
                4,
                8
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_18

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_18, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Owner name is either Mike, John or Tom'() {

        given:
        String cqlJson = """
        {
            "op": "in",
            "args": [
                {"property": "owner"},
                [ "Mike", "John", "Tom" ]
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_19

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_19, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'owner is NULL'() {

        given:
        String cqlJson = """
        {
            "op": "isNull",
            "args": [
                {"property": "owner"}
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_20

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_20, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'owner is not NULL'() {

        given:
        String cqlJson = """
        {
            "op": "not",
            "args": [
                {
                    "op": "isNull",
                    "args": [
                        {"property": "owner"}
                    ]
                }
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_21

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_21, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Built before 2015 (only date, no time information)'() {

        given:
        String cqlJson = """
        {
            "op": "t_before",
            "args": [
                {"property": "built"},
                {"date": "2015-01-01"}
            ]
        }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_24

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_24, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Happens between 7:30am June 10, 2017 and 10:30am June 11, 2017'() {

        given:
        String cqlJson = """
            {
                "op": "t_intersects",
                "args": [
                    {"interval": [{"property": "start"},{"property": "end"}]},
                    {"interval": ["2017-06-10T07:30:00Z","2017-06-11T10:30:00Z"]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_25x

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_25x, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Happens between June 10, 2017 and June 11, 2017'() {

        given:
        String cqlJson = """
            {
                "op": "t_intersects",
                "args": [
                    {"interval": [{"property": "start"},{"property": "end"}]},
                    {"interval": ["2017-06-10","2017-06-11"]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_25y

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_25y, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Happens June 10, 2017 or later'() {

        given:
        String cqlJson = """
            {
                "op": "t_intersects",
                "args": [
                    {"interval": [{"property": "start"},".."]},
                    {"interval": ["2017-06-10",".."]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_25z

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_25z, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

    def 'Updated between June 10, 2017 and June 11, 2017'() {

        given:
        String cqlJson = """
            {
                "op": "t_during",
                "args": [
                    {"property": "updated"},
                    {"interval": ["2017-06-10T07:30:00Z","2017-06-11T10:30:00Z"]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_25

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_25, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)

    }

   def 'Updated between 7:30am June 10, 2017 and an open end date'() {
        given:
        String cqlJson = """
            {
                "op": "t_during",
                "args": [
                    {"property": "updated"},
                    {"interval": ["2017-06-10T07:30:00Z",".."]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_26

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_26, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Updated between an open start date and 10:30am June 11, 2017'() {
        given:
        String cqlJson = """
            {
                "op": "t_during",
                "args": [
                    {"property": "updated"},
                    {"interval": ["..","2017-06-11T10:30:00Z"]}
               ] 
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_27

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_27, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Open interval on both ends'() {
        given:
        String cqlJson = """
            {
                "op": "t_during",
                "args": [
                    {"property": "updated"},
                    {"interval": ["..",".."]}
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_28

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_28, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Function with no arguments'() {
        given:
        String cqlJson = """
            {
                "op": "=",
                "args": [
                    {"function": {
                        "name": "pos",
                        "args": []
                    }},
                    1
                ]
            } 
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_29

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_29, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Function with multiple arguments'() {
        given:
        String cqlJson = """
            {
                "op": ">=",
                "args": [
                    {"function": {
                        "name": "indexOf",
                        "args": [ { "property": "names" }, "Mike" ]
                    }},
                    5
                ]
            } 
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_30

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_30, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Function with a temporal argument'() {
        given:
        String cqlJson = """
            {
                "op": "=",
                "args": [
                    {"function": {
                        "name": "year",
                        "args": [{"timestamp": "2012-06-05T00:00:00Z"}]
                    }},
                    2012
                ]
            } 
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_31

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_31, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Case insensitive string comparison function CASEI'() {
        given:
        String cqlJson = """
            {
              "op": "in",
              "args": [
                { "casei": { "property": "road_class" } },
                [
                  { "casei": "Οδος" },
                  { "casei": "Straße" }
                ]
              ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_CASEI

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_CASEI, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'Accent insensitive string comparison function ACCENTI'() {
        given:
        String cqlJson = """
            {
              "op": "in",
              "args": [
                { "accenti": { "property": "road_class" } },
                [
                  { "accenti": "Οδος" },
                  { "accenti": "Straße" }
                ]
              ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_ACCENTI

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_ACCENTI, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'UPPER'() {
        given:
        String cqlJson = """
            {
              "op": "in",
              "args": [
                { "function": { "name": "upper", "args": [ { "property": "road_class" } ] } },
                [ "A", "B", "L", "K" ]
              ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_UPPER

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_UPPER, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'LOWER'() {
        given:
        String cqlJson = """
            {
              "op": "in",
              "args": [
                { "function": { "name": "lower", "args": [ { "property": "road_class" } ] } },
                [ "a", "b", "l", "k" ]
              ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == CqlFilterExamples.EXAMPLE_LOWER

        and:

        when: 'writing json'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_LOWER, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'concurrent reads keep the CRS of each filter'() {
        given:
        String filter = '{"op":"s_intersects","args":[{"property":"geometry"},{"type":"Point","coordinates":[455000,5543000]}]}'
        List<EpsgCrs> crsList = (0..<200).collect { it % 2 == 0 ? EpsgCrs.of(25832) : OgcCrs.CRS84 }
        ExecutorService executor = Executors.newFixedThreadPool(8)

        when:
        List<Future<EpsgCrs>> results = crsList.collect { crs ->
            executor.submit({
                Cql2Expression expression = cql.read(filter, Cql.Format.JSON, crs)
                SpatialLiteral literal = (SpatialLiteral) ((BinarySpatialOperation) expression).getArgs().get(1)
                ((GeometryNode) literal.getValue()).getGeometry().getCrs().orElse(null)
            } as Callable<EpsgCrs>)
        }
        List<EpsgCrs> actual = results.collect { it.get() }

        then:
        actual == crsList

        cleanup:
        executor.shutdownNow()
    }
}
