/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.geometries.domain.Axes
import de.ii.xtraplatform.geometries.domain.MultiPoint
import de.ii.xtraplatform.geometries.domain.MultiPolygon
import de.ii.xtraplatform.geometries.domain.Point
import de.ii.xtraplatform.geometries.domain.Polygon
import de.ii.xtraplatform.geometries.domain.Position
import de.ii.xtraplatform.geometries.domain.PositionList

import java.util.stream.Collectors
import java.util.stream.Stream

import static de.ii.xtraplatform.features.domain.SchemaBase.Type

class FeatureTokenFixtures {

    private static final ObjectMapper YAML = YamlSerialization.createYamlMapper();

    public static FeatureTokens fromYaml(String name) {
        def resource = Resources.getResource("feature-tokens/" + name + ".yml");
        return YAML.readValue(Resources.toByteArray(resource), FeatureTokens.class);
    }

    public static List<Object> withType(String type, List<Object> tokens) {
        return tokens.stream().flatMap {
            if (it instanceof List) {
                return Stream.of([type] + ((List<String>) it))
            }
            if (it == FeatureTokenType.FEATURE) {
                return [it, [type]].stream()
            }
            return Stream.of(it)
        }.collect(Collectors.toList());
    }

    public static final List<Object> SINGLE_FEATURE = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_SOURCE = withType("biotop", SINGLE_FEATURE)

    public static final List<Object> SINGLE_FEATURE_POINT = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.GEOMETRY,
            ["geometry"],
            Point.of(Position.ofXY(8.18523495507722, 49.698295103021096), Optional.of(OgcCrs.CRS84)),
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_POINT_SOURCE = withType("biotop", SINGLE_FEATURE_POINT)

    public static final List<Object> SINGLE_FEATURE_MULTI_POINT = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "20",
            Type.STRING,
            FeatureTokenType.GEOMETRY,
            ["geometry"],
            MultiPoint.of(List.of(Point.of(Position.ofXY(6.406233970262905, 50.1501333536934), Optional.of(OgcCrs.CRS84)), Point.of(Position.ofXY(7.406233970262905, 51.1501333536934), Optional.of(OgcCrs.CRS84))), Optional.of(OgcCrs.CRS84)),
            FeatureTokenType.VALUE,
            ["kennung"],
            "580410003-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_MULTI_POINT_SOURCE = withType("biotop", SINGLE_FEATURE_MULTI_POINT)

    public static final List<Object> SINGLE_FEATURE_MULTI_POLYGON = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "21",
            Type.STRING,
            FeatureTokenType.GEOMETRY,
            ["geometry"],
            MultiPolygon.of(
                    List.of(
                            Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{8.18523495507722, 49.698295103021096, 8.185283687843047, 49.69823291309017, 8.185283687843047, 49.69823291309017, 8.18523495507722, 49.698295103021096})), Optional.of(OgcCrs.CRS84)),
                            Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{8.185681115675656, 49.698286680057166, 8.185796151881165, 49.69836248910692, 8.186313615874417, 49.698603368350874, 8.18641074595947, 49.69866280390489, 8.185681115675656, 49.698286680057166})), Optional.of(OgcCrs.CRS84))
                    ), Optional.of(OgcCrs.CRS84)),
            FeatureTokenType.VALUE,
            ["kennung"],
            "631510001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_MULTI_POLYGON_SOURCE = withType("biotop", SINGLE_FEATURE_MULTI_POLYGON)

    public static final List<Object> SINGLE_FEATURE_POLYGON_INNER = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["bag:pand"],
            FeatureTokenType.VALUE,
            ["bag:pand", "gml:@id"],
            "pand.4bad2970-2214-486c-a475-a04300136d94",
            Type.STRING,
            FeatureTokenType.OBJECT,
            ["bag:pand", "bag:geom"],
            GeometryType.POLYGON,
            FeatureTokenType.ARRAY,
            ["bag:pand", "bag:geom", "gml:Polygon"],
            FeatureTokenType.VALUE,
            ["bag:pand", "bag:geom", "gml:Polygon", "gml:exterior", "gml:LinearRing", "gml:posList"],
            "\n" +
                    "                206476.339000 475299.692000 206475.466000 475298.741000 206465.324000 475308.48000\n" +
                    "                206456.806000 475208.767000 206469.818000 475286.826000 206468.873000 475285.762000\n" +
                    "                206470.851000 475284.005000 206471.768000 475285.037000 206471.924000 475285.206000\n" +
                    "                206473.055000 475285.251000 206473.224000 475285.096000 206474.219000 475284.173000\n" +
                    "                206475.856000 475285.939000 206474.852000 475286.870000 206481.919000 475294.571000\n" +
                    "                206476.339000 475299.692000\n" +
                    "              ",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["bag:pand", "bag:geom", "gml:Polygon", "gml:interior", "gml:LinearRing", "gml:posList"],
            "\n" +
                    "                206475.281000 475298.053000 206477.379000 475296.023000 206474.715000 475293.270000\n" +
                    "                206472.617000 475295.300000 206475.281000 475298.053000\n" +
                    "              ",
            Type.STRING,
            FeatureTokenType.ARRAY_END,
            ["bag:pand", "bag:geom", "gml:Polygon"],
            FeatureTokenType.OBJECT_END,
            ["bag:pand", "bag:geom"],
            FeatureTokenType.VALUE,
            ["bag:pand", "bag:identificatie"],
            "0150100000004952",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.OBJECT,
            ["erfasser"],
            FeatureTokenType.VALUE,
            ["erfasser", "name"],
            "John Doe",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["erfasser"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_SOURCE = withType("biotop", SINGLE_FEATURE_NESTED_OBJECT)

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_EMPTY = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.OBJECT,
            ["erfasser"],
            FeatureTokenType.OBJECT_END,
            ["erfasser"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_EMPTY_REQUIRED = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.OBJECT,
            ["erfasser_required"],
            FeatureTokenType.OBJECT_END,
            ["erfasser_required"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["erfasser_array"],
            FeatureTokenType.VALUE,
            ["erfasser_array"],
            "John Doe",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["erfasser_array"],
            "Jane Doe",
            Type.STRING,
            FeatureTokenType.ARRAY_END,
            ["erfasser_array"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY_SOURCE = withType("biotop", SINGLE_FEATURE_VALUE_ARRAY)

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY_EMPTY = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["erfasser_array"],
            FeatureTokenType.ARRAY_END,
            ["erfasser_array"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY_EMPTY_REQUIRED = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["erfasser_array_required"],
            FeatureTokenType.ARRAY_END,
            ["erfasser_array_required"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY_AT_END = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "24",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["biotop", "[eid=id]erfasser"],
            FeatureTokenType.VALUE,
            ["biotop", "[eid=id]erfasser", "name"],
            "John Doe",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "[eid=id]erfasser", "name"],
            "Jane Doe",
            Type.STRING,
            FeatureTokenType.ARRAY_END,
            ["biotop", "[eid=id]erfasser"],
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY_IN_ORDER = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["biotop", "[eid=id]erfasser"],
            FeatureTokenType.VALUE,
            ["biotop", "[eid=id]erfasser", "name"],
            "John Doe",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "[eid=id]erfasser", "name"],
            "Jane Doe",
            Type.STRING,
            FeatureTokenType.ARRAY_END,
            ["biotop", "[eid=id]erfasser"],
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_VALUE_ARRAY_IN_ORDER_MAPPED = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["erfasser_array_join"],
            FeatureTokenType.VALUE,
            ["erfasser_array_join"],
            "John Doe",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["erfasser_array_join"],
            "Jane Doe",
            Type.STRING,
            FeatureTokenType.ARRAY_END,
            ["erfasser_array_join"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_ARRAYS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["raumreferenz"],
            FeatureTokenType.OBJECT,
            ["raumreferenz"],
            FeatureTokenType.ARRAY,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.OBJECT,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.VALUE,
            ["raumreferenz", "ortsangabe", "kreisschluessel"],
            "11",
            Type.INTEGER,
            FeatureTokenType.ARRAY,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            "34",
            Type.INTEGER,
            FeatureTokenType.ARRAY_END,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            FeatureTokenType.OBJECT_END,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.OBJECT,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.ARRAY,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            "35",
            Type.INTEGER,
            FeatureTokenType.VALUE,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            "36",
            Type.INTEGER,
            FeatureTokenType.ARRAY_END,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            FeatureTokenType.OBJECT_END,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.OBJECT,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.VALUE,
            ["raumreferenz", "ortsangabe", "kreisschluessel"],
            "12",
            Type.INTEGER,
            FeatureTokenType.ARRAY,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            "37",
            Type.INTEGER,
            FeatureTokenType.ARRAY_END,
            ["raumreferenz", "ortsangabe", "flurstueckskennzeichen"],
            FeatureTokenType.OBJECT_END,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.ARRAY_END,
            ["raumreferenz", "ortsangabe"],
            FeatureTokenType.OBJECT_END,
            ["raumreferenz"],
            FeatureTokenType.ARRAY_END,
            ["raumreferenz"],
            FeatureTokenType.VALUE,
            ["kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_ARRAYS_SOURCE = withType("biotop", SINGLE_FEATURE_NESTED_OBJECT_ARRAYS)

    public static final List<Object> OBJECT_WITHOUT_SOURCE_PATH = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["explorationsite", "id"],
            "24",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["explorationsite", "legalavailability_fk"],
            "11",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["explorationsite", "legalavailability_fk"],
            "11",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> OBJECT_WITHOUT_SOURCE_PATH_MAPPED = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.OBJECT,
            ["legalAvailability"],
            FeatureTokenType.VALUE,
            ["legalAvailability", "title"],
            "11",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["legalAvailability", "href"],
            "11",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["legalAvailability"],
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> EXPLORATION_SITE_OBJECT_ARRAY = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["explorationsite", "id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.OBJECT,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.VALUE,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task", "projectname"],
            "11",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task", "id"],
            "34",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.OBJECT,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.VALUE,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task", "projectname"],
            "35",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task", "id"],
            "36",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.OBJECT,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.VALUE,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task", "projectname"],
            "12",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task", "id"],
            "37",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.ARRAY_END,
            ["explorationsite", "[id=explorationsite_fk]explorationsite_task", "[task_fk=id]task"],
            FeatureTokenType.VALUE,
            ["explorationsite", "kennung"],
            "611320001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> EXPLORATION_SITE_OBJECT_ARRAY_MAPPED = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "24",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["task"],
            FeatureTokenType.OBJECT,
            ["task"],
            FeatureTokenType.VALUE,
            ["task", "title"],
            "11",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["task", "id"],
            "34",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["task"],
            FeatureTokenType.OBJECT,
            ["task"],
            FeatureTokenType.VALUE,
            ["task", "title"],
            "35",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["task", "id"],
            "36",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["task"],
            FeatureTokenType.OBJECT,
            ["task"],
            FeatureTokenType.VALUE,
            ["task", "title"],
            "12",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["task", "id"],
            "37",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["task"],
            FeatureTokenType.ARRAY_END,
            ["task"],
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> COLLECTION = [
            FeatureTokenType.INPUT,
            3L,
            12L,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "19",
            Type.STRING,
            FeatureTokenType.GEOMETRY,
            ["geometry"],
            Point.of(Position.ofXY(6.295202392345018, 50.11336914792363), Optional.of(OgcCrs.CRS84)),
            FeatureTokenType.VALUE,
            ["kennung"],
            "580340001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "20",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["kennung"],
            "580410003-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.FEATURE,
            FeatureTokenType.VALUE,
            ["id"],
            "21",
            Type.STRING,
            FeatureTokenType.GEOMETRY,
            ["geometry"],
            MultiPoint.of(List.of(Point.of(Position.ofXY(6.406233970262905, 50.1501333536934), Optional.of(OgcCrs.CRS84))), Optional.of(OgcCrs.CRS84)),
            FeatureTokenType.VALUE,
            ["kennung"],
            "631510001-1",
            Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<Object> COLLECTION_SOURCE = withType("biotop", COLLECTION)

    public static final List<Object> STUFF = [
            FeatureTokenType.VALUE,
            ["oid"],
            "17e2d34d-44e0-44af-9de9-d8f8d8a6408e",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["hatGenerAttribut"],
            FeatureTokenType.OBJECT,
            ["hatGenerAttribut"],
            FeatureTokenType.VALUE,
            ["hatGenerAttribut", "name"],
            "drehwinkel",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["hatGenerAttribut", "wert"],
            "1.0",
            Type.FLOAT,
            FeatureTokenType.VALUE,
            ["hatGenerAttribut", "wert"],
            "bar",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["hatGenerAttribut"],
            FeatureTokenType.OBJECT,
            ["hatGenerAttribut"],
            FeatureTokenType.VALUE,
            ["hatGenerAttribut", "name"],
            "foo",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["hatGenerAttribut", "wert"],
            "bar",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["hatGenerAttribut"],
            FeatureTokenType.ARRAY_END,
            ["hatGenerAttribut"],
            FeatureTokenType.OBJECT,
            ["gehoertZuBereich"],
            FeatureTokenType.VALUE,
            ["gehoertZuBereich", "id"],
            "d9bfd586-3e00-488f-8686-fd8387f6fc35",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["gehoertZuBereich", "type"],
            "BP_Bereich",
            Type.STRING,
            FeatureTokenType.VALUE,
            ["gehoertZuBereich", "title"],
            "0",
            Type.INTEGER,
            FeatureTokenType.OBJECT_END,
            ["gehoertZuBereich"],
            FeatureTokenType.VALUE,
            ["rechtscharakter"],
            "1000",
            Type.STRING,
            FeatureTokenType.OBJECT,
            ["position"],
            GeometryType.POINT,
            2,
            FeatureTokenType.VALUE,
            ["position"],
            "561524.729 5942848.073",
            Type.STRING,
            FeatureTokenType.OBJECT_END,
            ["position"],
            FeatureTokenType.VALUE,
            ["nordwinkel"],
            "0.0",
            Type.FLOAT,
            FeatureTokenType.VALUE,
            ["massnahme"],
            "1000",
            Type.STRING,
            FeatureTokenType.ARRAY,
            ["gegenstand"],
            FeatureTokenType.VALUE,
            ["gegenstand"],
            "1000",
            Type.STRING,
            FeatureTokenType.ARRAY_END,
            ["gegenstand"],
            FeatureTokenType.VALUE,
            ["istAusgleich"],
            "false",
            Type.BOOLEAN
    ]
}
