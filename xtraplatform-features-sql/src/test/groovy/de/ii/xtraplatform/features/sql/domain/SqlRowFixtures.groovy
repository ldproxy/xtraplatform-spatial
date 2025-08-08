/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import de.ii.xtraplatform.features.domain.FeatureStoreAttributesContainer
import de.ii.xtraplatform.features.domain.FeatureStoreInstanceContainer
import de.ii.xtraplatform.features.domain.FeatureStoreRelatedContainer
import de.ii.xtraplatform.features.domain.FeatureStoreRelation
import de.ii.xtraplatform.features.domain.FeatureStoreTypeInfo
import de.ii.xtraplatform.features.domain.FeatureTokenType
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreAttribute
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreInstanceContainer
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreRelatedContainer
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreRelation
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreTypeInfo
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.geometries.domain.GeometryType

class SqlRowFixtures {

    public static FeatureStoreRelatedContainer ERFASSER_CONTAINER = ImmutableFeatureStoreRelatedContainer.builder()
            .name("erfasser")
            .sortKey("id")
            .addInstanceConnection(ImmutableFeatureStoreRelation.builder()
                .cardinality(FeatureStoreRelation.CARDINALITY.ONE_2_ONE)
                .sourceContainer("biotop")
                .sourceField("id")
                .targetContainer("erfasser")
                .targetField("id")
                .build())
            .addAttributes(ImmutableFeatureStoreAttribute.builder()
                    .name("name")
                    .path(["biotop", "[id=id]erfasser", "name"])
                    .build())
            .build()

    public static FeatureStoreRelatedContainer ORTSANGABE_CONTAINER = ImmutableFeatureStoreRelatedContainer.builder()
            .name("ortsangabe")
            .sortKey("id")
            .addInstanceConnection(ImmutableFeatureStoreRelation.builder()
                    .cardinality(FeatureStoreRelation.CARDINALITY.M_2_N)
                    .sourceContainer("biotop")
                    .sourceField("id")
                    .junctionSource("biotop_id")
                    .junction("biotop_2_raumreferenz")
                    .junctionTarget("raumreferenz_id")
                    .targetContainer("raumreferenz")
                    .targetField("id")
                    .build())
            .addInstanceConnection(ImmutableFeatureStoreRelation.builder()
                    .cardinality(FeatureStoreRelation.CARDINALITY.M_2_N)
                    .sourceContainer("raumreferenz")
                    .sourceField("id")
                    .junctionSource("raumreferenz_id")
                    .junction("raumreferenz_2_ortsangabe")
                    .junctionTarget("ortsangabe_id")
                    .targetContainer("ortsangabe")
                    .targetField("id")
                    .build())
            .addAttributes(ImmutableFeatureStoreAttribute.builder()
                    .name("kreisschluessel")
                    .path(["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "kreisschluessel"])
                    .build())
            .build()

    public static FeatureStoreRelatedContainer FSK_CONTAINER = ImmutableFeatureStoreRelatedContainer.builder()
            .name("flurstueckskennzeichen")
            .sortKey("id")
            .addInstanceConnection(ImmutableFeatureStoreRelation.builder()
                    .cardinality(FeatureStoreRelation.CARDINALITY.M_2_N)
                    .sourceContainer("biotop")
                    .sourceField("id")
                    .junctionSource("biotop_id")
                    .junction("biotop_2_raumreferenz")
                    .junctionTarget("raumreferenz_id")
                    .targetContainer("raumreferenz")
                    .targetField("id")
                    .build())
            .addInstanceConnection(ImmutableFeatureStoreRelation.builder()
                    .cardinality(FeatureStoreRelation.CARDINALITY.M_2_N)
                    .sourceContainer("raumreferenz")
                    .sourceField("id")
                    .junctionSource("raumreferenz_id")
                    .junction("raumreferenz_2_ortsangabe")
                    .junctionTarget("ortsangabe_id")
                    .targetContainer("ortsangabe")
                    .targetField("id")
                    .build())
            .addInstanceConnection(ImmutableFeatureStoreRelation.builder()
                    .cardinality(FeatureStoreRelation.CARDINALITY.ONE_2_ONE)
                    .sourceContainer("ortsangabe")
                    .sourceField("id")
                    .targetContainer("flurstueckskennzeichen")
                    .targetField("ortsangabe_id")
                    .build())
            .addAttributes(ImmutableFeatureStoreAttribute.builder()
                    .name("flurstueckskennzeichen")
                    .path(["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen", "flurstueckskennzeichen"])
                    .build())
            .build()

    public static FeatureStoreInstanceContainer BIOTOP_CONTAINER = ImmutableFeatureStoreInstanceContainer.builder()
            .name("biotop")
            .sortKey("id")
            .addAttributes(ImmutableFeatureStoreAttribute.builder()
                    .name("id")
                    .path(["biotop", "id"])
                    .build())
            .addAttributes(ImmutableFeatureStoreAttribute.builder()
                    .name("geometry")
                    .path(["biotop", "geometry"])
                    .isSpatial(true)
                    .build())
            .addAttributes(ImmutableFeatureStoreAttribute.builder()
                    .name("kennung")
                    .path(["biotop", "kennung"])
                    .build())
            .addRelatedContainers(ERFASSER_CONTAINER)
            .addRelatedContainers(ORTSANGABE_CONTAINER)
            .addRelatedContainers(FSK_CONTAINER)
            .build()

    public static List<FeatureStoreTypeInfo> TYPE_INFOS = [
            ImmutableFeatureStoreTypeInfo.builder()
                    .name("biotop")
                    .addInstanceContainers(BIOTOP_CONTAINER)
                    .build()
    ]

    public static final List<SqlRow> SINGLE_FEATURE = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(1)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [24],
                    ["24", null, "611320001-1"]
            )
    ]

    public static final List<Object> SINGLE_FEATURE_TOKENS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "24",
            SchemaBase.Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "611320001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<SqlRow> SINGLE_FEATURE_POINT = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(1)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [24],
                    ["24", "POINT(8.18523495507722 49.698295103021096)", "611320001-1"]
            )
    ]

    public static final List<Object> SINGLE_FEATURE_POINT_TOKENS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "24",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT,
            ["biotop", "geometry"],
            GeometryType.POINT,
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "8.18523495507722 49.698295103021096",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "611320001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<SqlRow> SINGLE_FEATURE_MULTI_POINT = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(1)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [20],
                    ["20", "MULTIPOINT(6.406233970262905 50.1501333536934,7.406233970262905 51.1501333536934)", "580410003-1"]
            )
    ]

    public static final List<Object> SINGLE_FEATURE_MULTI_POINT_TOKENS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "20",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT,
            ["biotop", "geometry"],
            GeometryType.MULTI_POINT,
            FeatureTokenType.ARRAY,
            ["biotop", "geometry"],
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "6.406233970262905 50.1501333536934",
            SchemaBase.Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "7.406233970262905 51.1501333536934",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "580410003-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<SqlRow> SINGLE_FEATURE_MULTI_POLYGON = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(1)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [21],
                    ["21", "MULTIPOLYGON(((8.18523495507722 49.698295103021096, 8.185283687843047 49.69823291309017)),((8.185681115675656 49.698286680057166, 8.185796151881165 49.69836248910692),(8.186313615874417 49.698603368350874, 8.18641074595947 49.69866280390489)))", "631510001-1"]
            )
    ]

    public static final List<Object> SINGLE_FEATURE_MULTI_POLYGON_TOKENS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "21",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT,
            ["biotop", "geometry"],
            GeometryType.MULTI_POLYGON,
            FeatureTokenType.ARRAY,
            ["biotop", "geometry"],
            FeatureTokenType.ARRAY,
            ["biotop", "geometry"],
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "8.18523495507722 49.698295103021096, 8.185283687843047 49.69823291309017",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.ARRAY,
            ["biotop", "geometry"],
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "8.185681115675656 49.698286680057166, 8.185796151881165 49.69836248910692",
            SchemaBase.Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "8.186313615874417 49.698603368350874, 8.18641074595947 49.69866280390489",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "631510001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<SqlRow> SINGLE_FEATURE_NESTED_OBJECT = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(1)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [24],
                    ["24", null, "611320001-1"]
            ),
            new SqlRowMock(
                    ERFASSER_CONTAINER,
                    [24, 24],
                    ["John Doe"]
            )
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_TOKENS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "24",
            SchemaBase.Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "611320001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY,
            ["biotop", "[id=id]erfasser"],
            FeatureTokenType.OBJECT,
            ["biotop", "[id=id]erfasser"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=id]erfasser", "name"],
            "John Doe",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<SqlRow> SINGLE_FEATURE_NESTED_OBJECT_ARRAYS = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(1)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [24],
                    ["24", null, "611320001-1"]
            ),
            new SqlRowMock(
                    ORTSANGABE_CONTAINER,
                    [24, 1],
                    ["11"]
            ),
            new SqlRowMock(
                    FSK_CONTAINER,
                    [24, 1, 1],
                    ["34"]
            ),
            new SqlRowMock(
                    FSK_CONTAINER,
                    [24, 2, 1],
                    ["35"]
            ),
            new SqlRowMock(
                    FSK_CONTAINER,
                    [24, 2, 2],
                    ["36"]
            ),
            new SqlRowMock(
                    ORTSANGABE_CONTAINER,
                    [24, 3],
                    ["12"]
            ),
            new SqlRowMock(
                    FSK_CONTAINER,
                    [24, 3, 1],
                    ["37"]
            )
    ]

    public static final List<Object> SINGLE_FEATURE_NESTED_OBJECT_ARRAYS_TOKENS = [
            FeatureTokenType.INPUT,
            true,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "24",
            SchemaBase.Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "611320001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe"],
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "kreisschluessel"],
            "11",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen", "flurstueckskennzeichen"],
            "34",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe"],
            FeatureTokenType.ARRAY,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen", "flurstueckskennzeichen"],
            "35",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen", "flurstueckskennzeichen"],
            "36",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "kreisschluessel"],
            "12",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.OBJECT,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen"],
            FeatureTokenType.VALUE,
            ["biotop", "[id=biotop_id]biotop_2_raumreferenz", "[raumreferenz_id=id]raumreferenz", "[id=raumreferenz_id]raumreferenz_2_ortsangabe", "[ortsangabe_id=id]ortsangabe", "[id=ortsangabe_id]flurstueckskennzeichen", "flurstueckskennzeichen"],
            "37",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    public static final List<SqlRow> COLLECTION = [
            new ImmutableSqlRowMeta.Builder()
                    .numberReturned(3)
                    .numberMatched(12)
                    .build(),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [19],
                    ["19", "POINT(6.295202392345018 50.11336914792363)", "580340001-1"]
            ),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [20],
                    ["20", null, "580410003-1"]
            ),
            new SqlRowMock(
                    BIOTOP_CONTAINER,
                    [21],
                    ["21", "MULTIPOINT(6.406233970262905 50.1501333536934)", "631510001-1"]
            )
    ]

    public static final List<Object> COLLECTION_TOKENS = [
            FeatureTokenType.INPUT,
            3L,
            12L,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "19",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT,
            ["biotop", "geometry"],
            GeometryType.POINT,
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "6.295202392345018 50.11336914792363",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "580340001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "20",
            SchemaBase.Type.STRING,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "580410003-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.FEATURE,
            ["biotop"],
            FeatureTokenType.VALUE,
            ["biotop", "id"],
            "21",
            SchemaBase.Type.STRING,
            FeatureTokenType.OBJECT,
            ["biotop", "geometry"],
            GeometryType.MULTI_POINT,
            FeatureTokenType.ARRAY,
            ["biotop", "geometry"],
            FeatureTokenType.VALUE,
            ["biotop", "geometry"],
            "6.406233970262905 50.1501333536934",
            SchemaBase.Type.STRING,
            FeatureTokenType.ARRAY_END,
            FeatureTokenType.OBJECT_END,
            FeatureTokenType.VALUE,
            ["biotop", "kennung"],
            "631510001-1",
            SchemaBase.Type.STRING,
            FeatureTokenType.FEATURE_END,
            FeatureTokenType.INPUT_END
    ]

    static class SqlRowMock implements SqlRow {

        FeatureStoreAttributesContainer attributesContainer
        List<Comparable<?>> ids
        List<Object> values

        SqlRowMock(FeatureStoreAttributesContainer attributesContainer, List<Long> ids, List<String> values) {
            this.attributesContainer = attributesContainer
            this.ids = ids
            this.values = values
        }

        @Override
        List<Object> getValues() {
            return values
        }

        @Override
        List<String> getPath() {
            return attributesContainer.getPath();
        }

        @Override
        List<Comparable<?>> getIds() {
            return ids
        }

        @Override
        Optional<String> getType() {
            return Optional.of('ft')
        }

        @Override
        List<List<String>> getColumnPaths() {
            return attributesContainer.getAttributePaths();
        }

        @Override
        boolean isSpatialColumn(int i) {
            return attributesContainer.getAttributes().get(i).isSpatial()
        }
    }
}
