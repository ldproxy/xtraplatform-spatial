/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zahnen
 */
@SuppressWarnings({"PMD.UseUtilityClass", "PMD.FieldNamingConventions"})
public class VersionedVocabulary {

  public enum NAMESPACE {
    PREFIX,
    URI;
  }

  protected static final Map<Class, Map<Enum, Map<Enum, String>>> vocabulary;
  private static final Class[] children = {
    WFS.class, FES.class, GML.class, XLINK.class, XSI.class, CSW.class, OWS.class
  };

  static {
    vocabulary = new HashMap<>();

    // TODO: this is a dirty hack to execute the static initializers of the subclasses
    for (Class c : children) {
      try {
        c.newInstance();
      } catch (InstantiationException | IllegalAccessException ex) {
        // ignore
      }
    }
  }

  protected static void addWord(Enum v, Enum w, String s) {
    Class c = v.getDeclaringClass().getEnclosingClass();
    if (!vocabulary.containsKey(c)) {
      vocabulary.put(c, new HashMap<Enum, Map<Enum, String>>());
    }
    if (!vocabulary.get(c).containsKey(v)) {
      vocabulary.get(c).put(v, new HashMap<Enum, String>());
    }
    for (Map.Entry<Enum, Map<Enum, String>> e : vocabulary.get(c).entrySet()) {
      if (e.getKey().equals(v) || e.getKey().compareTo(v) > 0) {
        vocabulary.get(c).get(e.getKey()).put(w, s);
      }
    }
  }

  public static String getWord(Enum v, Enum w) {
    if (v == null) {
      return getWord(w);
    }

    String word = "";
    Class c = v.getDeclaringClass().getEnclosingClass();
    if (vocabulary.containsKey(c)
        && vocabulary.get(c).containsKey(v)
        && vocabulary.get(c).get(v).containsKey(w)) {
      word = vocabulary.get(c).get(v).get(w);
    }
    return word;
  }

  public static String getWord(Enum w) {
    Class c = w.getDeclaringClass().getEnclosingClass();
    if (vocabulary.containsKey(c)) {
      for (Map<Enum, String> voc : vocabulary.get(c).values()) {
        if (voc.containsKey(w)) {
          return voc.get(w);
        }
      }
    }
    return "";
  }

  public static String getQN(Enum v, Enum w) {
    return getWord(v, NAMESPACE.PREFIX) + ":" + getWord(v, w);
  }

  public static String getNS(Enum v) {
    return getWord(v, NAMESPACE.URI);
  }

  public static String getPR(Enum v) {
    return getWord(v, NAMESPACE.PREFIX);
  }
}
