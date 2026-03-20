/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public class CoordinateTupleWithPrecision extends CoordinateTuple {

  private static DecimalFormat DEFAULT_FORMAT =
      new DecimalFormat("#.0###########", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
  private static DecimalFormat METRIC_FORMAT =
      new DecimalFormat("#.0##", DecimalFormatSymbols.getInstance(Locale.ENGLISH));

  private final DecimalFormat formatter;

  public CoordinateTupleWithPrecision(double[] c, boolean isTargetMetric) {
    super(c);
    this.formatter = isTargetMetric ? METRIC_FORMAT : DEFAULT_FORMAT;
  }

  @Override
  public String getXasString() {
    return formatter.format(c[0]);
  }

  @Override
  public String getYasString() {
    return formatter.format(c[1]);
  }
}
