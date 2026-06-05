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
public class GML extends VersionedVocabulary {

  public enum VERSION {

    // IMPORTANT! keep the order of the items!
    _2_1_1("2.1.1"),
    _3_1_1("3.1.1"),
    _3_2_1_OLD("3.2.1.OLD"),
    _3_2_1("3.2.1");

    private final String stringRepresentation;

    VERSION(String stringRepresentation) {
      this.stringRepresentation = stringRepresentation;
    }

    @Override
    public String toString() {
      return stringRepresentation;
    }

    public static VERSION fromString(String version) {
      for (VERSION v : VERSION.values()) {
        if (v.toString().equals(version)) {
          return v;
        }
      }
      return null;
    }

    public static VERSION fromOutputFormatString(String value) {

      String normalized = value.toLowerCase(java.util.Locale.ROOT);

      if (normalized.contains("gml2")) {
        return VERSION._2_1_1;
      } else if (normalized.contains("text/xml") && normalized.contains("subtype=gml/3.1.1")) {
        return VERSION._3_1_1;
      } else if (normalized.contains("text/xml") && normalized.contains("subtype=gml/3.2.1")) {
        return VERSION._3_2_1_OLD;
      } else if (normalized.contains("application/gml+xml") && normalized.contains("version=3.2")) {
        return VERSION._3_2_1;
      }

      return null;
    }

    public boolean isGreaterOrEqual(GML.VERSION other) {
      return this.compareTo(other) >= 0;
    }

    public boolean isGreater(GML.VERSION other) {
      return this.compareTo(other) > 0;
    }
  }

  public enum VOCABULARY {
    ENVELOPE,
    SRSNAME,
    LOWER_CORNER,
    UPPER_CORNER,
    ABSTRACT_OBJECT,
    ABSTRACT_FEATURE,
    OUTPUTFORMAT_VALUE,
    POLYGON,
    EXTERIOR,
    LINEAR_RING,
    POS_LIST,
    SRSDIMENSION,
    GMLID,
    TIME_PERIOD,
    BEGIN_POSITION,
    END_POSITION,
    TIME_INSTANT,
    TIME_POSITION;
  }

  static {
    // IMPORTANT! make the order of the VERSIONS like in enum VERSION above!

    // Values in lower versions are automatically present in higher versions
    // if not overwritten.

    addWord(VERSION._2_1_1, NAMESPACE.PREFIX, "gml");
    addWord(VERSION._2_1_1, NAMESPACE.URI, "http://www.opengis.net/gml");

    addWord(VERSION._3_1_1, NAMESPACE.PREFIX, "gml");
    addWord(VERSION._3_1_1, NAMESPACE.URI, "http://www.opengis.net/gml");

    addWord(VERSION._3_2_1, NAMESPACE.PREFIX, "gml");
    addWord(VERSION._3_2_1, NAMESPACE.URI, "http://www.opengis.net/gml/3.2");

    addWord(VERSION._2_1_1, VOCABULARY.ABSTRACT_FEATURE, "_Feature");
    addWord(VERSION._2_1_1, VOCABULARY.ABSTRACT_OBJECT, "_Object");
    addWord(VERSION._2_1_1, VOCABULARY.ENVELOPE, "Envelope");
    addWord(VERSION._2_1_1, VOCABULARY.SRSNAME, "srsName");
    addWord(VERSION._2_1_1, VOCABULARY.LOWER_CORNER, "lowerCorner");
    addWord(VERSION._2_1_1, VOCABULARY.UPPER_CORNER, "upperCorner");
    addWord(
        VERSION._2_1_1, VOCABULARY.OUTPUTFORMAT_VALUE, "GML2"); // text/xml; subtype=gml/2.1.1");
    addWord(VERSION._2_1_1, VOCABULARY.POLYGON, "Polygon");
    addWord(VERSION._2_1_1, VOCABULARY.EXTERIOR, "exterior");
    addWord(VERSION._2_1_1, VOCABULARY.LINEAR_RING, "LinearRing");
    addWord(VERSION._2_1_1, VOCABULARY.POS_LIST, "posList");
    addWord(VERSION._2_1_1, VOCABULARY.SRSDIMENSION, "srsDimension");
    addWord(VERSION._2_1_1, VOCABULARY.GMLID, "gml:id");
    addWord(VERSION._2_1_1, VOCABULARY.TIME_PERIOD, "TimePeriod");
    addWord(VERSION._2_1_1, VOCABULARY.BEGIN_POSITION, "beginPosition");
    addWord(VERSION._2_1_1, VOCABULARY.END_POSITION, "endPosition");
    addWord(VERSION._2_1_1, VOCABULARY.TIME_INSTANT, "TimeInstant");
    addWord(VERSION._2_1_1, VOCABULARY.TIME_POSITION, "timePosition");

    addWord(VERSION._3_1_1, VOCABULARY.OUTPUTFORMAT_VALUE, "text/xml; subtype=gml/3.1.1");

    addWord(VERSION._3_2_1_OLD, VOCABULARY.ABSTRACT_FEATURE, "AbstractFeature");
    addWord(VERSION._3_2_1_OLD, VOCABULARY.ABSTRACT_OBJECT, "AbstractObject");
    addWord(VERSION._3_2_1_OLD, VOCABULARY.OUTPUTFORMAT_VALUE, "text/xml; subtype=gml/3.2.1");

    addWord(VERSION._3_2_1, VOCABULARY.OUTPUTFORMAT_VALUE, "application/gml+xml; version=3.2");
  }
}
