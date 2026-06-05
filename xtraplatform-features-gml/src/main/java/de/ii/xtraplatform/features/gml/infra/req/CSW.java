/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import org.apache.hc.core5.net.URIBuilder;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public class CSW extends VersionedVocabulary {

  public enum VERSION {
    _2_0_0("2.0.0", FES.VERSION._1_1_0),
    _2_0_2("2.0.2", FES.VERSION._1_1_0);

    private final String stringRepresentation;
    private final FES.VERSION filterVersion;

    VERSION(String stringRepresentation, FES.VERSION filterVersion) {
      this.stringRepresentation = stringRepresentation;
      this.filterVersion = filterVersion;
    }

    @Override
    public String toString() {
      return stringRepresentation;
    }

    public FES.VERSION getFilterVersion() {
      return filterVersion;
    }

    public static VERSION fromString(String version) {
      for (VERSION v : VERSION.values()) {
        if (v.toString().equals(version)) {
          return v;
        }
      }
      return null;
    }

    public boolean isGreaterOrEqual(VERSION other) {
      return this.compareTo(other) >= 0;
    }

    public boolean isGreater(VERSION other) {
      return this.compareTo(other) > 0;
    }
  }

  public static boolean hasKVPKey(String key) {

    String[] keys = {
      "REQUEST",
      "SERVICE",
      "VERSION",
      "NAMESPACES",
      "TYPENAMES",
      "COUNT",
      "NAMESPACE",
      "TYPENAME",
      "MAXFEATURES",
      "OUTPUT_FORMAT"
    };

    return Arrays.asList(keys).contains(key);
  }

  public enum OPERATION {
    GET_CAPABILITES("GetCapabilities"),
    GET_RECORDS("GetRecords"),
    NONE("");
    private final String stringRepresentation;

    OPERATION(String stringRepresentation) {
      this.stringRepresentation = stringRepresentation;
    }

    @Override
    public String toString() {
      return stringRepresentation;
    }

    public static OPERATION fromString(String type) {
      for (OPERATION v : OPERATION.values()) {
        if (v.toString().equals(type)) {
          return v;
        }
      }
      return NONE;
    }
  }

  public enum METHOD {
    GET("GET"),
    POST("POST");
    private final String stringRepresentation;

    METHOD(String stringRepresentation) {
      this.stringRepresentation = stringRepresentation;
    }

    @Override
    public String toString() {
      return stringRepresentation;
    }

    public static METHOD fromString(String type) {
      for (METHOD v : METHOD.values()) {
        if (v.toString().equals(type)) {
          return v;
        }
      }
      return null;
    }
  }

  public enum VOCABULARY {
    NOT_A_WORD,
    TYPENAMES,
    VERSION,
    MAXRECORDS,
    STARTPOSITION,
    GET_RECORDS,
    GET_CAPABILITES,
    OUTPUT_FORMAT,
    QUERY,
    RESULTTYPE,
    HITS,
    SERVICE,
    CSW,
    REQUEST,
    NAMESPACES,
    EXCEPTION_REPORT,
    EXCEPTION,
    EXCEPTION_CODE,
    CONSTRAINTLANGUAGE,
    RESULTS,
    OPERATIONS_METADATA,
    OPERATION,
    GET,
    POST,
    ELEMENTSETNAME,
    OUPUTSCHEMA,
    CONSTRAINT_LANGUAGE_VERSION,
    DCP,
    PARAMETER,
    CONSTRAINT,
    VALUE,
    DEFAULT_VALUE,
    ONLINE_RESOURCE_ATTRIBUTE,
    RESULT_FORMAT,
    NEXT_RECORD,
    SEARCH_RESULTS,
    NUMBER_OF_RECORDS_MATCHED,
    NUMBER_OF_RECORDS_RETURNED,
    MD_METADATA,
    IDENTIFICATION_INFO,
    SV_SERVICE_IDENTIFICATION,
    TYPE,
    SERVICE_TYPE,
    SERVICE_TYPE_VERSION,
    CONTAINS_OPERATIONS,
    SV_OPERATION_METADATA,
    OPERATION_NAME,
    CONNECT_POINT,
    CI_ONLINE_RESOURCE,
    LINKAGE,
    PROTOCOL;
  }

  static {
    addWord(VERSION._2_0_0, NAMESPACE.PREFIX, "csw");
    addWord(VERSION._2_0_0, NAMESPACE.URI, "http://www.opengis.net/cat/csw");

    addWord(VERSION._2_0_2, NAMESPACE.PREFIX, "csw");
    addWord(VERSION._2_0_2, NAMESPACE.URI, "http://www.opengis.net/cat/csw/2.0.2");

    addWord(VERSION._2_0_0, OPERATION.GET_RECORDS, "GetRecords");
    addWord(VERSION._2_0_0, OPERATION.GET_CAPABILITES, "GetCapabilities");

    addWord(VERSION._2_0_0, VOCABULARY.QUERY, "Query");
    addWord(VERSION._2_0_0, VOCABULARY.TYPENAMES, "typeNames");
    addWord(VERSION._2_0_0, VOCABULARY.NAMESPACES, "namespaces");
    addWord(VERSION._2_0_0, VOCABULARY.VERSION, "version");
    addWord(VERSION._2_0_0, VOCABULARY.MAXRECORDS, "maxRecords");
    addWord(VERSION._2_0_0, VOCABULARY.STARTPOSITION, "startPosition");
    addWord(VERSION._2_0_0, VOCABULARY.OUTPUT_FORMAT, "outputFormat");
    addWord(VERSION._2_0_0, VOCABULARY.RESULTTYPE, "resultType");
    addWord(VERSION._2_0_0, VOCABULARY.HITS, "hits");
    addWord(VERSION._2_0_0, VOCABULARY.EXCEPTION_REPORT, "ServiceExceptionReport");
    addWord(VERSION._2_0_0, VOCABULARY.EXCEPTION, "ServiceException");
    addWord(VERSION._2_0_0, VOCABULARY.EXCEPTION_CODE, "code");
    addWord(VERSION._2_0_0, VOCABULARY.OPERATIONS_METADATA, "Capability");
    addWord(VERSION._2_0_0, VOCABULARY.OPERATION, "Request");
    addWord(VERSION._2_0_0, VOCABULARY.DCP, "DCPType");
    addWord(VERSION._2_0_0, VOCABULARY.ONLINE_RESOURCE_ATTRIBUTE, "onlineResource");
    addWord(VERSION._2_0_0, VOCABULARY.RESULT_FORMAT, "ResultFormat");
    addWord(VERSION._2_0_0, VOCABULARY.GET, "Get");
    addWord(VERSION._2_0_0, VOCABULARY.POST, "Post");
    addWord(VERSION._2_0_0, VOCABULARY.DCP, "DCP");
    addWord(VERSION._2_0_0, VOCABULARY.PARAMETER, "Parameter");
    addWord(VERSION._2_0_0, VOCABULARY.CONSTRAINT, "Constraint");
    addWord(VERSION._2_0_0, VOCABULARY.VALUE, "Value");
    addWord(VERSION._2_0_0, VOCABULARY.DEFAULT_VALUE, "DefaultValue");
    addWord(VERSION._2_0_0, VOCABULARY.SERVICE, "service");
    addWord(VERSION._2_0_0, VOCABULARY.CSW, "CSW");
    addWord(VERSION._2_0_0, VOCABULARY.REQUEST, "request");
    addWord(VERSION._2_0_0, VOCABULARY.CONSTRAINTLANGUAGE, "ConstraintLanguage");
    addWord(VERSION._2_0_0, VOCABULARY.RESULTS, "results");
    addWord(VERSION._2_0_0, VOCABULARY.ELEMENTSETNAME, "ElementSetName");
    addWord(VERSION._2_0_0, VOCABULARY.OUPUTSCHEMA, "outputSchema");
    addWord(VERSION._2_0_0, VOCABULARY.CONSTRAINT_LANGUAGE_VERSION, "CONSTRAINT_LANGUAGE_VERSION");

    addWord(VERSION._2_0_0, VOCABULARY.SEARCH_RESULTS, "SearchResults");
    addWord(VERSION._2_0_0, VOCABULARY.NUMBER_OF_RECORDS_MATCHED, "numberOfRecordsMatched");
    addWord(VERSION._2_0_0, VOCABULARY.NUMBER_OF_RECORDS_RETURNED, "numberOfRecordsReturned");
    addWord(VERSION._2_0_0, VOCABULARY.NEXT_RECORD, "nextRecord");
    addWord(VERSION._2_0_0, VOCABULARY.MD_METADATA, "MD_Metadata");
    addWord(VERSION._2_0_0, VOCABULARY.IDENTIFICATION_INFO, "identificationInfo");
    addWord(VERSION._2_0_0, VOCABULARY.SV_SERVICE_IDENTIFICATION, "SV_ServiceIdentification");
    addWord(VERSION._2_0_0, VOCABULARY.TYPE, "Type");
    addWord(VERSION._2_0_0, VOCABULARY.SERVICE_TYPE, "serviceType");
    addWord(VERSION._2_0_0, VOCABULARY.SERVICE_TYPE_VERSION, "serviceTypeVersion");
    addWord(VERSION._2_0_0, VOCABULARY.CONTAINS_OPERATIONS, "containsOperations");
    addWord(VERSION._2_0_0, VOCABULARY.SV_OPERATION_METADATA, "SV_OperationMetadata");
    addWord(VERSION._2_0_0, VOCABULARY.OPERATION_NAME, "operationName");
    addWord(VERSION._2_0_0, VOCABULARY.CONNECT_POINT, "connectPoint");
    addWord(VERSION._2_0_0, VOCABULARY.CI_ONLINE_RESOURCE, "CI_OnlineResource");
    addWord(VERSION._2_0_0, VOCABULARY.LINKAGE, "linkage");
    addWord(VERSION._2_0_0, VOCABULARY.PROTOCOL, "protocol");
  }

  public static VOCABULARY findKey(String word) {
    for (VERSION v : VERSION.values()) {
      for (Map.Entry<Enum, String> e : vocabulary.get(CSW.class).get(v).entrySet()) {
        if (e.getKey() instanceof VOCABULARY && e.getValue().equals(word)) {
          return (VOCABULARY) e.getKey();
        }
      }
    }
    return VOCABULARY.NOT_A_WORD;
  }

  public static String cleanUrl(String url) {
    try {
      URI inUri = new URI(url.trim());
      URIBuilder outUri = new URIBuilder(inUri).removeQuery();

      if (inUri.getQuery() != null && !inUri.getQuery().isEmpty()) {
        for (String inParam : inUri.getQuery().split("&")) {
          String[] param = inParam.split("=");
          if (!CSW.hasKVPKey(param[0].toUpperCase(Locale.ROOT))) {
            outUri.addParameter(param[0], param[1]);
          }
        }
      }

      return outUri.toString();
    } catch (URISyntaxException ex) {
      return url;
    }
  }
}
