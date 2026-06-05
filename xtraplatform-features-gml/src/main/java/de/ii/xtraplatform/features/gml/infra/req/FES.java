/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public class FES extends VersionedVocabulary {

  public enum VERSION {
    _1_0_0("1.0.0", GML.VERSION._2_1_1),
    _1_1_0("1.1.0", GML.VERSION._3_1_1),
    _2_0_0("2.0.0", GML.VERSION._3_2_1);

    private final String stringRepresentation;
    private final GML.VERSION gmlVersion;

    VERSION(String stringRepresentation, GML.VERSION gmlVersion) {
      this.stringRepresentation = stringRepresentation;
      this.gmlVersion = gmlVersion;
    }

    @Override
    public String toString() {
      return stringRepresentation;
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

    public boolean isGreaterOrEqual(FES.VERSION other) {
      return this.compareTo(other) >= 0;
    }

    public boolean isEqual(FES.VERSION other) {
      return this.compareTo(other) == 0;
    }
  }

  public enum VOCABULARY {
    AND,
    OR,
    BBOX,
    VALUE_REFERENCE,
    LESS,
    LESSEQUAL,
    GREATER,
    GREATEREQUAL,
    EQUAL,
    NOTEQUAL,
    NOT,
    LITERAL,
    FILTER,
    RESOURCEID,
    RESOURCEID_ATTR,
    LIKE,
    NOTLIKE,
    WILD_CARD,
    SINGLE_CHAR,
    ESCAPE_CHAR,
    RESOURCEID_KVP,
    BETWEEN,
    UPPERBOUNDARY,
    LOWERBOUNDARY,
    ISNULL,
    INTERSECTS,
    DURING;
  }

  static {
    addWord(VERSION._1_0_0, NAMESPACE.PREFIX, "ogc");
    addWord(VERSION._1_0_0, NAMESPACE.URI, "http://www.opengis.net/ogc");

    addWord(VERSION._1_1_0, NAMESPACE.PREFIX, "ogc");
    addWord(VERSION._1_1_0, NAMESPACE.URI, "http://www.opengis.net/ogc");

    addWord(VERSION._2_0_0, NAMESPACE.PREFIX, "fes");
    addWord(VERSION._2_0_0, NAMESPACE.URI, "http://www.opengis.net/fes/2.0");

    addWord(VERSION._1_0_0, VOCABULARY.AND, "And");
    addWord(VERSION._1_0_0, VOCABULARY.OR, "Or");
    addWord(VERSION._1_0_0, VOCABULARY.NOT, "Not");
    addWord(VERSION._1_0_0, VOCABULARY.BBOX, "BBOX");
    addWord(VERSION._1_0_0, VOCABULARY.VALUE_REFERENCE, "PropertyName");
    addWord(VERSION._1_0_0, VOCABULARY.LESS, "PropertyIsLessThan");
    addWord(VERSION._1_0_0, VOCABULARY.LESSEQUAL, "PropertyIsLessThanOrEqualTo");
    addWord(VERSION._1_0_0, VOCABULARY.GREATER, "PropertyIsGreaterThan");
    addWord(VERSION._1_0_0, VOCABULARY.GREATEREQUAL, "PropertyIsGreaterThanOrEqualTo");
    addWord(VERSION._1_0_0, VOCABULARY.EQUAL, "PropertyIsEqualTo");
    addWord(VERSION._1_0_0, VOCABULARY.NOTEQUAL, "PropertyIsNotEqualTo");
    addWord(VERSION._1_0_0, VOCABULARY.LIKE, "PropertyIsLike");
    addWord(VERSION._1_0_0, VOCABULARY.NOTLIKE, "PropertyIsNotLike");
    addWord(VERSION._1_0_0, VOCABULARY.BETWEEN, "PropertyIsBetween");
    addWord(VERSION._1_0_0, VOCABULARY.LOWERBOUNDARY, "LowerBoundary");
    addWord(VERSION._1_0_0, VOCABULARY.UPPERBOUNDARY, "UpperBoundary");
    addWord(VERSION._1_0_0, VOCABULARY.ISNULL, "PropertyIsNull");
    addWord(VERSION._1_0_0, VOCABULARY.RESOURCEID, "FeatureId");
    addWord(VERSION._1_0_0, VOCABULARY.RESOURCEID_KVP, "FEATUREID");
    addWord(VERSION._1_0_0, VOCABULARY.RESOURCEID_ATTR, "fid");
    addWord(VERSION._1_0_0, VOCABULARY.LITERAL, "Literal");
    addWord(VERSION._1_0_0, VOCABULARY.WILD_CARD, "wildCard");
    addWord(VERSION._1_0_0, VOCABULARY.SINGLE_CHAR, "singleChar");
    addWord(VERSION._1_0_0, VOCABULARY.ESCAPE_CHAR, "escapeChar");
    addWord(VERSION._1_0_0, VOCABULARY.FILTER, "Filter");
    addWord(VERSION._1_0_0, VOCABULARY.INTERSECTS, "Intersects");

    addWord(VERSION._1_1_0, VOCABULARY.RESOURCEID, "GmlObjectId");
    addWord(VERSION._1_1_0, VOCABULARY.RESOURCEID_ATTR, "id");

    addWord(VERSION._2_0_0, VOCABULARY.VALUE_REFERENCE, "ValueReference");
    addWord(VERSION._2_0_0, VOCABULARY.RESOURCEID, "ResourceId");
    addWord(VERSION._2_0_0, VOCABULARY.RESOURCEID_KVP, "RESOURCEID");
    addWord(VERSION._2_0_0, VOCABULARY.RESOURCEID_ATTR, "rid");
    addWord(VERSION._2_0_0, VOCABULARY.DURING, "During");
  }
}
