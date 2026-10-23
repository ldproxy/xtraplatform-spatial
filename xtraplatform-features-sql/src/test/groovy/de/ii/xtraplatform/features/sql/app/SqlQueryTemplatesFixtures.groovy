/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app


import de.ii.xtraplatform.features.domain.Tuple

class SqlQueryTemplatesFixtures {

    static String META = "WITH\n" +
            "    NR AS (SELECT MIN(SKEY) AS minKey, MAX(SKEY) AS maxKey, count(*) AS numberReturned FROM (SELECT A.id AS SKEY FROM externalprovider A ORDER BY SKEY LIMIT 10 OFFSET 10) AS IDS),\n" +
            "    NM AS (SELECT count(*) AS numberMatched FROM (SELECT A.id AS SKEY FROM externalprovider A ORDER BY 1) AS IDS),\n" +
            "    NS AS (SELECT -1::bigint AS numberSkipped)\n" +
            "  SELECT * FROM NR, NM, NS"

    static String META_WITHOUT_NUMBER_MATCHED = "WITH\n" +
                    "    NR AS (SELECT MIN(SKEY) AS minKey, MAX(SKEY) AS maxKey, count(*) AS numberReturned FROM (SELECT A.id AS SKEY FROM externalprovider A ORDER BY SKEY LIMIT 10 OFFSET 10) AS IDS),\n" +
                    "    NM AS (SELECT -1::bigint AS numberMatched),\n" +
                    "    NS AS (SELECT -1::bigint AS numberSkipped)\n" +
                    "  SELECT * FROM NR, NM, NS"

    static String META_SORT_BY = "WITH\n" +
            "    NR AS (SELECT NULL AS minKey, NULL AS maxKey, count(*) AS numberReturned FROM (SELECT A.created AS CSKEY_0, A.id AS SKEY FROM externalprovider A ORDER BY CSKEY_0, SKEY LIMIT 10 OFFSET 10) AS IDS),\n" +
            "    NM AS (SELECT count(*) AS numberMatched FROM (SELECT A.id AS SKEY FROM externalprovider A ORDER BY 1) AS IDS),\n" +
            "    NS AS (SELECT -1::bigint AS numberSkipped)\n" +
            "  SELECT * FROM NR, NM, NS"

    static String META_SORT_BY_DESC = "WITH\n" +
            "    NR AS (SELECT NULL AS minKey, NULL AS maxKey, count(*) AS numberReturned FROM (SELECT A.created AS CSKEY_0, A.id AS SKEY FROM externalprovider A ORDER BY CSKEY_0 DESC, SKEY LIMIT 10 OFFSET 10) AS IDS),\n" +
            "    NM AS (SELECT count(*) AS numberMatched FROM (SELECT A.id AS SKEY FROM externalprovider A ORDER BY 1) AS IDS),\n" +
            "    NS AS (SELECT -1::bigint AS numberSkipped)\n" +
            "  SELECT * FROM NR, NM, NS"

    static String META_SORT_BY_MIXED = "WITH\n" +
            "    NR AS (SELECT NULL AS minKey, NULL AS maxKey, count(*) AS numberReturned FROM (SELECT A.created AS CSKEY_0, A.lastModified AS CSKEY_1, A.id AS SKEY FROM externalprovider A ORDER BY CSKEY_0 DESC, CSKEY_1, SKEY LIMIT 10 OFFSET 10) AS IDS),\n" +
            "    NM AS (SELECT count(*) AS numberMatched FROM (SELECT A.id AS SKEY FROM externalprovider A ORDER BY 1) AS IDS),\n" +
            "    NS AS (SELECT -1::bigint AS numberSkipped)\n" +
            "  SELECT * FROM NR, NM, NS"

    static String META_FILTER = "WITH\n" +
            "    NR AS (SELECT MIN(SKEY) AS minKey, MAX(SKEY) AS maxKey, count(*) AS numberReturned FROM (SELECT A.id AS SKEY FROM externalprovider A WHERE A.type = 1 ORDER BY SKEY LIMIT 10 OFFSET 10) AS IDS),\n" +
            "    NM AS (SELECT count(*) AS numberMatched FROM (SELECT A.id AS SKEY FROM externalprovider A WHERE A.type = 1 ORDER BY 1) AS IDS),\n" +
            "    NS AS (SELECT -1::bigint AS numberSkipped)\n" +
            "  SELECT * FROM NR, NM, NS"

    static List<String> VALUE_ARRAY = [
            "SELECT A.id AS SKEY, A.id FROM externalprovider A ORDER BY 1",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.externalprovidername FROM externalprovider A JOIN externalprovider_externalprovidername B ON (A.id=B.externalprovider_fk) ORDER BY 1,2"
    ]

    static List<String> OBJECT_ARRAY = [
            "SELECT A.id AS SKEY, A.id FROM explorationsite A ORDER BY 1",
            "SELECT A.id AS SKEY, C.id AS SKEY_1, C.id, C.projectname, C.id FROM explorationsite A JOIN explorationsite_task B ON (A.id=B.explorationsite_fk) JOIN task C ON (B.task_fk=C.id) ORDER BY 1,2"
    ]

    static List<String> MERGE = [
            "SELECT A.id AS SKEY, A.programm FROM eignungsflaeche A ORDER BY 1",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.id, B.kennung FROM eignungsflaeche A JOIN osirisobjekt B ON (A.id=B.id) ORDER BY 1,2"
    ]

    static List<String> SELF_JOINS = [
            "SELECT A.id AS SKEY, A.id FROM building A ORDER BY 1",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.id FROM building A JOIN building B ON (A.id=B.fk_buildingpart_parent) ORDER BY 1,2",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.id FROM building A JOIN building B ON (A.fk_buildingpart_parent=B.id) ORDER BY 1,2"
    ]

    static List<String> SELF_JOINS_FILTER = [
            "SELECT A.id AS SKEY, A.oid FROM building A WHERE (A.oid > 1) ORDER BY 1",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.id FROM building A JOIN building B ON (A.id=B.fk_buildingpart_parent AND (B.id > 100)) WHERE (A.oid > 1) ORDER BY 1,2",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.id FROM building A JOIN building B ON (A.fk_buildingpart_parent=B.id AND (B.id > 1000)) WHERE (A.oid > 1) ORDER BY 1,2"
    ]

    static List<String> SELF_JOIN_NESTED_DUPLICATE = [
            "SELECT A.id AS SKEY, A.id FROM building A ORDER BY 1",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B.name FROM building A JOIN att_string_building B ON (A.id=B.fk_feature) ORDER BY 1,2",
            "SELECT A.id AS SKEY, C.id AS SKEY_1, C.name FROM building A JOIN building B ON (A.id=B.fk_buildingpart_parent) JOIN att_string_building C ON (B.id=C.fk_feature) ORDER BY 1,2"
    ]

    static List<String> OBJECT_WITHOUT_SOURCE_PATH = [
            "SELECT A.id AS SKEY, A.id, A.legalavailability_fk, A.legalavailability_fk FROM explorationsite A ORDER BY 1"
    ]

    static List<String> OBJECT_ARRAY_PAGING = [
            "SELECT A.id AS SKEY, A.id FROM explorationsite A WHERE (A.id >= 10 AND A.id <= 19) ORDER BY 1",
            "SELECT A.id AS SKEY, C.id AS SKEY_1, C.id, C.projectname, C.id FROM explorationsite A JOIN explorationsite_task B ON (A.id=B.explorationsite_fk) JOIN task C ON (B.task_fk=C.id) WHERE (A.id >= 10 AND A.id <= 19) ORDER BY 1,2"
    ]

    static List<String> OBJECT_ARRAY_SORTBY = [
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, A.id FROM explorationsite A ORDER BY 1,2",
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, C.id AS SKEY_1, C.id, C.projectname, C.id FROM explorationsite A JOIN explorationsite_task B ON (A.id=B.explorationsite_fk) JOIN task C ON (B.task_fk=C.id) ORDER BY 1,2,3"
    ]

    static List<String> OBJECT_ARRAY_SORTBY_FILTER = [
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, A.id FROM explorationsite A WHERE (A.id IN (SELECT AA.id FROM explorationsite AA JOIN explorationsite_task AB ON (AA.id=AB.explorationsite_fk) JOIN task AC ON (AB.task_fk=AC.id) WHERE AC.projectname = 'foo')) ORDER BY 1,2",
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, C.id AS SKEY_1, C.id, C.projectname, C.id FROM explorationsite A JOIN explorationsite_task B ON (A.id=B.explorationsite_fk) JOIN task C ON (B.task_fk=C.id) WHERE (A.id IN (SELECT AA.id FROM explorationsite AA JOIN explorationsite_task AB ON (AA.id=AB.explorationsite_fk) JOIN task AC ON (AB.task_fk=AC.id) WHERE AC.projectname = 'foo')) ORDER BY 1,2,3"
    ]

    static List<String> OBJECT_ARRAY_SORTBY_PAGING = [
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, A.id FROM explorationsite A ORDER BY 1,2 LIMIT 10 OFFSET 10",
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, C.id AS SKEY_1, C.id, C.projectname, C.id FROM explorationsite A JOIN explorationsite_task B ON (A.id=B.explorationsite_fk) JOIN task C ON (B.task_fk=C.id) WHERE (A.id IN (SELECT AA.id FROM explorationsite AA ORDER BY AA.created,AA.id LIMIT 10 OFFSET 10)) ORDER BY 1,2,3"
    ]

    static List<String> OBJECT_ARRAY_SORTBY_PAGING_FILTER = [
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, A.id FROM explorationsite A WHERE ((A.id IN (SELECT AA.id FROM explorationsite AA JOIN explorationsite_task AB ON (AA.id=AB.explorationsite_fk) JOIN task AC ON (AB.task_fk=AC.id) WHERE AC.projectname = 'foo') AND A.id IN (SELECT AA.id FROM explorationsite AA JOIN explorationsite_task AB ON (AA.id=AB.explorationsite_fk) JOIN task AC ON (AB.task_fk=AC.id) WHERE AC.id = 'bar'))) ORDER BY 1,2 LIMIT 10 OFFSET 10",
            "SELECT A.created AS CSKEY_0, A.id AS SKEY, C.id AS SKEY_1, C.id, C.projectname, C.id FROM explorationsite A JOIN explorationsite_task B ON (A.id=B.explorationsite_fk) JOIN task C ON (B.task_fk=C.id) WHERE (A.id IN (SELECT AAA.id FROM explorationsite AAA WHERE ((AAA.id IN (SELECT AA.id FROM explorationsite AA JOIN explorationsite_task AB ON (AA.id=AB.explorationsite_fk) JOIN task AC ON (AB.task_fk=AC.id) WHERE AC.projectname = 'foo') AND AAA.id IN (SELECT AA.id FROM explorationsite AA JOIN explorationsite_task AB ON (AA.id=AB.explorationsite_fk) JOIN task AC ON (AB.task_fk=AC.id) WHERE AC.id = 'bar'))) ORDER BY AAA.created,AAA.id LIMIT 10 OFFSET 10)) ORDER BY 1,2,3"
    ]

    static List<String> PROPERTY_WITH_MULTIPLE_SOURCE_PATHS = [
            "SELECT A.id AS SKEY, A.objid, A.lan, A.rbz, A.krs, A.gmd FROM o12006 A ORDER BY 1"
    ]

    static List<String> NESTED_JOINS = [
            "SELECT A.id AS SKEY, A.id FROM observationsubject A ORDER BY 1",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, B._type FROM observationsubject A JOIN observationsubject_filtervalues B ON (A.id=B.observationsubjectid) ORDER BY 1,2",
            "SELECT A.id AS SKEY, B.id AS SKEY_1, C.id AS SKEY_2, C.symbol, C.code FROM observationsubject A JOIN observationsubject_filtervalues B ON (A.id=B.observationsubjectid) JOIN observedproperty C ON (B.filtervalueproperty_fk=C.code) ORDER BY 1,2,3"
    ]

}
