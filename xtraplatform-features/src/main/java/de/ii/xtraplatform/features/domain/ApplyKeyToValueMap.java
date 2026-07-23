/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ForwardingMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ApplyKeyToValueMap<T> extends ForwardingMap<String, T> {

  private final Map<String, T> delegate;
  private final BiFunction<String, T, T> applyKeyToValue;

  public ApplyKeyToValueMap(Map<String, T> delegate, BiFunction<String, T, T> applyKeyToValue) {
    super();
    this.delegate = delegate;
    this.applyKeyToValue = applyKeyToValue;
  }

  @Override
  protected Map<String, T> delegate() {
    return delegate;
  }

  @Override
  public T put(String key, T value) {
    return delegate.put(key, applyKeyToValue.apply(key, value));
  }
}
