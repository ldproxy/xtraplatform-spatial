/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import java.util.Map;

/**
 * @author fischer
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public class OWS extends VersionedVocabulary {

  public enum VERSION {
    _1_0_0("1.0.0", FES.VERSION._1_0_0, GML.VERSION._2_1_1),
    _1_1_0("1.1.0", FES.VERSION._1_1_0, GML.VERSION._3_1_1),
    _2_0_0("2.0.0", FES.VERSION._2_0_0, GML.VERSION._3_2_1);
    private final String stringRepresentation;
    private final FES.VERSION filterVersion;
    private final GML.VERSION gmlVersion;

    VERSION(String stringRepresentation, FES.VERSION filterVersion, GML.VERSION gmlVersion) {
      this.stringRepresentation = stringRepresentation;
      this.filterVersion = filterVersion;
      this.gmlVersion = gmlVersion;
    }

    @Override
    public String toString() {
      return stringRepresentation;
    }

    public FES.VERSION getFilterVersion() {
      return filterVersion;
    }

    public GML.VERSION getGmlVersion() {
      return gmlVersion;
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

  public enum OPERATION {
    GET_CAPABILITES("GetCapabilities"),
    DESCRIBE_FEATURE_TYPE("DescribeFeatureType"),
    GET_FEATURE("GetFeature"),
    GET_PROPERTY_VALUE("GetPropertyValue"),
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
    QUERY,
    TYPENAMES,
    SRSNAME,
    VERSION,
    COUNT,
    STARTINDEX,
    GET_FEATURE,
    GET_CAPABILITES,
    DESCRIBE_FEATURE_TYPE,
    OUTPUT_FORMAT,
    GET_PROPERTY_VALUE,
    VALUE_REFERENCE,
    EXCEPTION_REPORT,
    EXCEPTION,
    EXCEPTION_TEXT,
    EXCEPTION_CODE,
    SERVICE_IDENTIFICATION,
    SERVICE_PROVIDER,
    OPERATIONS_METADATA,
    FEATURE_TYPE_LIST,
    FEATURE_TYPE,
    NAME,
    TITLE,
    ABSTRACT,
    KEYWORDS,
    KEYWORD,
    FEES,
    ACCESS_CONSTRAINTS,
    SERVICE_TYPE_VERSION,
    PROVIDER_NAME,
    PROVIDER_SITE,
    SERVICE_CONTACT,
    INDIVIDUAL_NAME,
    ORGANIZATION_NAME,
    POSITION_NAME,
    CONTACT_INFO,
    ROLE,
    PHONE,
    ADDRESS,
    ONLINE_RESOURCE,
    HOURS_OF_SERVICE,
    CONTACT_INSTRUCTIONS,
    DELIVERY_POINT,
    VOICE,
    FACSIMILE,
    CITY,
    ADMINISTRATIVE_AREA,
    POSTAL_CODE,
    COUNTRY,
    EMAIL,
    OPERATION,
    NAME_ATTRIBUTE,
    GET,
    POST,
    DCP,
    PARAMETER,
    CONSTRAINT,
    METADATA,
    VALUE,
    DEFAULT_VALUE,
    ONLINE_RESOURCE_ATTRIBUTE,
    COUNT_DEFAULT,
    RESULT_FORMAT,
    DEFAULT_CRS,
    OTHER_CRS,
    WGS84_BOUNDING_BOX,
    METADATA_URL,
    LOWER_CORNER,
    UPPER_CORNER,
    MIN_X,
    MIN_Y,
    MAX_X,
    MAX_Y,
    EXTENDED_CAPABILITIES,
    INSPIRE_METADATA_URL,
    INSPIRE_URL,
    SUPPORTED_ISO_QUERYABLES,
    CONSTRAINTLANGUAGE,
    CONSTRAINT_LANGUAGE;
  }

  static {
    addWord(VERSION._1_0_0, NAMESPACE.PREFIX, "wfs");
    addWord(VERSION._1_0_0, NAMESPACE.URI, "http://www.opengis.net/wfs");

    addWord(VERSION._1_1_0, NAMESPACE.PREFIX, "wfs");
    addWord(VERSION._1_1_0, NAMESPACE.URI, "http://www.opengis.net/wfs");

    addWord(VERSION._2_0_0, NAMESPACE.PREFIX, "wfs");
    addWord(VERSION._2_0_0, NAMESPACE.URI, "http://www.opengis.net/wfs/2.0");

    addWord(VERSION._1_0_0, VOCABULARY.GET_FEATURE, "GetFeature");
    addWord(VERSION._1_0_0, VOCABULARY.GET_CAPABILITES, "GetCapabilities");
    addWord(VERSION._1_0_0, VOCABULARY.DESCRIBE_FEATURE_TYPE, "DescribeFeatureType");
    addWord(VERSION._1_0_0, VOCABULARY.GET_PROPERTY_VALUE, "GetPropertyValue");
    addWord(VERSION._1_0_0, VOCABULARY.VALUE_REFERENCE, "valueReference");
    addWord(VERSION._1_0_0, VOCABULARY.QUERY, "Query");
    addWord(VERSION._1_0_0, VOCABULARY.TYPENAMES, "typeName");
    addWord(VERSION._1_0_0, VOCABULARY.SRSNAME, "srsName");
    addWord(VERSION._1_0_0, VOCABULARY.VERSION, "version");
    addWord(VERSION._1_0_0, VOCABULARY.COUNT, "maxFeatures");
    addWord(VERSION._1_0_0, VOCABULARY.OUTPUT_FORMAT, "outputFormat");
    addWord(VERSION._1_0_0, VOCABULARY.EXCEPTION_REPORT, "ServiceExceptionReport");
    addWord(VERSION._1_0_0, VOCABULARY.EXCEPTION, "ServiceException");
    addWord(VERSION._1_0_0, VOCABULARY.EXCEPTION_CODE, "code");
    addWord(VERSION._1_0_0, VOCABULARY.SERVICE_IDENTIFICATION, "Service");
    addWord(VERSION._1_0_0, VOCABULARY.SERVICE_PROVIDER, "ServiceProvider");
    addWord(VERSION._1_0_0, VOCABULARY.OPERATIONS_METADATA, "Capability");
    addWord(VERSION._1_0_0, VOCABULARY.FEATURE_TYPE_LIST, "FeatureTypeList");
    addWord(VERSION._1_0_0, VOCABULARY.FEATURE_TYPE, "FeatureType");
    addWord(VERSION._1_0_0, VOCABULARY.NAME, "Name");
    addWord(VERSION._1_0_0, VOCABULARY.TITLE, "Title");
    addWord(VERSION._1_0_0, VOCABULARY.ABSTRACT, "Abstract");
    addWord(VERSION._1_0_0, VOCABULARY.KEYWORDS, "Keywords");
    addWord(VERSION._1_0_0, VOCABULARY.KEYWORD, "Keyword");
    addWord(VERSION._1_0_0, VOCABULARY.FEES, "Fees");
    addWord(VERSION._1_0_0, VOCABULARY.ACCESS_CONSTRAINTS, "AccessConstraints");
    addWord(VERSION._1_0_0, VOCABULARY.OPERATION, "Request");
    addWord(VERSION._1_0_0, VOCABULARY.DCP, "DCPType");
    addWord(VERSION._1_0_0, VOCABULARY.ONLINE_RESOURCE_ATTRIBUTE, "onlineResource");
    addWord(VERSION._1_0_0, VOCABULARY.RESULT_FORMAT, "ResultFormat");
    addWord(VERSION._1_0_0, VOCABULARY.OTHER_CRS, "SRS");
    addWord(VERSION._1_0_0, VOCABULARY.WGS84_BOUNDING_BOX, "LatLongBoundingBox");
    addWord(VERSION._1_0_0, VOCABULARY.METADATA_URL, "MetadataURL");
    addWord(VERSION._1_0_0, VOCABULARY.MIN_X, "minx");
    addWord(VERSION._1_0_0, VOCABULARY.MIN_Y, "miny");
    addWord(VERSION._1_0_0, VOCABULARY.MAX_X, "maxx");
    addWord(VERSION._1_0_0, VOCABULARY.MAX_Y, "maxy");

    addWord(VERSION._1_1_0, VOCABULARY.QUERY, "Query");
    addWord(VERSION._1_1_0, VOCABULARY.TYPENAMES, "typeName");
    addWord(VERSION._1_1_0, VOCABULARY.SRSNAME, "srsName");
    addWord(VERSION._1_1_0, VOCABULARY.VERSION, "version");
    addWord(VERSION._1_1_0, VOCABULARY.COUNT, "maxFeatures");
    addWord(VERSION._1_1_0, VOCABULARY.EXCEPTION_REPORT, "ExceptionReport");
    addWord(VERSION._1_1_0, VOCABULARY.EXCEPTION, "Exception");
    addWord(VERSION._1_1_0, VOCABULARY.EXCEPTION_TEXT, "ExceptionText");
    addWord(VERSION._1_1_0, VOCABULARY.EXCEPTION_CODE, "exceptionCode");
    addWord(VERSION._1_1_0, VOCABULARY.SERVICE_IDENTIFICATION, "ServiceIdentification");
    addWord(VERSION._1_1_0, VOCABULARY.OPERATIONS_METADATA, "OperationsMetadata");
    addWord(VERSION._1_1_0, VOCABULARY.SERVICE_TYPE_VERSION, "ServiceTypeVersion");
    addWord(VERSION._1_1_0, VOCABULARY.PROVIDER_NAME, "ProviderName");
    addWord(VERSION._1_1_0, VOCABULARY.PROVIDER_SITE, "ProviderSite");
    addWord(VERSION._1_1_0, VOCABULARY.SERVICE_CONTACT, "ServiceContact");
    addWord(VERSION._1_1_0, VOCABULARY.INDIVIDUAL_NAME, "IndividualName");
    addWord(VERSION._1_1_0, VOCABULARY.ORGANIZATION_NAME, "OrganisationName");
    addWord(VERSION._1_1_0, VOCABULARY.POSITION_NAME, "PositionName");
    addWord(VERSION._1_1_0, VOCABULARY.CONTACT_INFO, "ContactInfo");
    addWord(VERSION._1_1_0, VOCABULARY.ROLE, "Role");
    addWord(VERSION._1_1_0, VOCABULARY.PHONE, "Phone");
    addWord(VERSION._1_1_0, VOCABULARY.FACSIMILE, "Facsimile");
    addWord(VERSION._1_1_0, VOCABULARY.ADDRESS, "Address");
    addWord(VERSION._1_1_0, VOCABULARY.ONLINE_RESOURCE, "OnlineResource");
    addWord(VERSION._1_1_0, VOCABULARY.HOURS_OF_SERVICE, "HoursOfService");
    addWord(VERSION._1_1_0, VOCABULARY.CONTACT_INSTRUCTIONS, "ContactInstructions");
    addWord(VERSION._1_1_0, VOCABULARY.VOICE, "Voice");
    addWord(VERSION._1_1_0, VOCABULARY.DELIVERY_POINT, "DeliveryPoint");
    addWord(VERSION._1_1_0, VOCABULARY.CITY, "City");
    addWord(VERSION._1_1_0, VOCABULARY.ADMINISTRATIVE_AREA, "AdministrativeArea");
    addWord(VERSION._1_1_0, VOCABULARY.POSTAL_CODE, "PostalCode");
    addWord(VERSION._1_1_0, VOCABULARY.COUNTRY, "Country");
    addWord(VERSION._1_1_0, VOCABULARY.EMAIL, "ElectronicMailAddress");
    addWord(VERSION._1_1_0, VOCABULARY.OPERATION, "Operation");
    addWord(VERSION._1_1_0, VOCABULARY.NAME_ATTRIBUTE, "name");
    addWord(VERSION._1_1_0, VOCABULARY.GET, "Get");
    addWord(VERSION._1_1_0, VOCABULARY.POST, "Post");
    addWord(VERSION._1_1_0, VOCABULARY.DCP, "DCP");
    addWord(VERSION._1_1_0, VOCABULARY.PARAMETER, "Parameter");
    addWord(VERSION._1_1_0, VOCABULARY.CONSTRAINT, "Constraint");
    addWord(VERSION._1_1_0, VOCABULARY.METADATA, "Metadata");
    addWord(VERSION._1_1_0, VOCABULARY.VALUE, "Value");
    addWord(VERSION._1_1_0, VOCABULARY.DEFAULT_VALUE, "DefaultValue");
    addWord(VERSION._1_1_0, VOCABULARY.DEFAULT_CRS, "DefaultSRS");
    addWord(VERSION._1_1_0, VOCABULARY.OTHER_CRS, "OtherSRS");
    addWord(VERSION._1_1_0, VOCABULARY.WGS84_BOUNDING_BOX, "WGS84BoundingBox");
    addWord(VERSION._1_1_0, VOCABULARY.LOWER_CORNER, "LowerCorner");
    addWord(VERSION._1_1_0, VOCABULARY.UPPER_CORNER, "UpperCorner");

    addWord(VERSION._2_0_0, VOCABULARY.QUERY, "Query");
    addWord(VERSION._2_0_0, VOCABULARY.TYPENAMES, "typeNames");
    addWord(VERSION._2_0_0, VOCABULARY.SRSNAME, "srsName");
    addWord(VERSION._2_0_0, VOCABULARY.VERSION, "version");
    addWord(VERSION._2_0_0, VOCABULARY.COUNT, "count");
    addWord(VERSION._2_0_0, VOCABULARY.STARTINDEX, "startIndex");
    addWord(VERSION._2_0_0, VOCABULARY.COUNT_DEFAULT, "CountDefault");
    addWord(VERSION._2_0_0, VOCABULARY.DEFAULT_CRS, "DefaultCRS");
    addWord(VERSION._2_0_0, VOCABULARY.OTHER_CRS, "OtherCRS");
    addWord(VERSION._2_0_0, VOCABULARY.EXTENDED_CAPABILITIES, "ExtendedCapabilities");
    addWord(VERSION._2_0_0, VOCABULARY.INSPIRE_METADATA_URL, "MetadataUrl");
    addWord(VERSION._2_0_0, VOCABULARY.INSPIRE_URL, "URL");

    addWord(VERSION._2_0_0, VOCABULARY.SUPPORTED_ISO_QUERYABLES, "SupportedISOQueryables");
    addWord(VERSION._2_0_0, VOCABULARY.CONSTRAINT_LANGUAGE, "ConstraintLanguage");
    addWord(VERSION._2_0_0, VOCABULARY.CONSTRAINTLANGUAGE, "CONSTRAINTLANGUAGE");
  }

  public static VOCABULARY findKey(String word) {
    for (VERSION v : VERSION.values()) {
      for (Map.Entry<Enum, String> e : vocabulary.get(OWS.class).get(v).entrySet()) {
        if (e.getKey() instanceof VOCABULARY && e.getValue().equals(word)) {
          return (VOCABULARY) e.getKey();
        }
      }
    }
    return VOCABULARY.NOT_A_WORD;
  }
}
