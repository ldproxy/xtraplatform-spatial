/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.DecoderFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;

@Singleton
@AutoBind
public class DecoderFactorySqlExpression implements DecoderFactory {

  public static final MediaType MEDIA_TYPE =
      MediaType.valueOf("application/vnd.ldproxy.sql-expression");
  public static final String CONNECTOR_STRING = "EXPRESSION";
  private static final Pattern SQL_FLAG = Pattern.compile("\\{sql=(?<SQL>.+?)\\}");
  public static final String CONNECTOR_PATH_TEMPLATE = "[EXPRESSION]{sql=%s}";

  private final AtomicInteger expressionCounter = new AtomicInteger(0);
  private final Map<String, String> expressionMap = Collections.synchronizedMap(new HashMap<>());

  @Inject
  public DecoderFactorySqlExpression() {}

  @Override
  public MediaType getMediaType() {
    return MEDIA_TYPE;
  }

  @Override
  public Optional<String> getConnectorString() {
    return Optional.of(CONNECTOR_STRING);
  }

  @Override
  public Decoder createDecoder() {
    return Decoder.noop(CONNECTOR_STRING);
  }

  @Override
  public Tuple<String, String> parseSourcePath(
      String path, String column, String flags, String connectorSpec) {
    Matcher matcher = SQL_FLAG.matcher(flags);

    if (matcher.find()) {
      String key = String.format("SQL__%s", expressionCounter.incrementAndGet());
      String expression = matcher.group("SQL");

      expressionMap.put(key, expression);

      return Tuple.of(key, expression);
    }

    return DecoderFactory.super.parseSourcePath(path, column, flags, connectorSpec);
  }

  @Override
  public List<String> resolvePath(List<String> path) {
    if (path.isEmpty()) {
      return path;
    }

    String key = path.get(path.size() - 1).replace("[" + CONNECTOR_STRING + "]", "");

    if (expressionMap.containsKey(key)) {
      String expression = expressionMap.get(key);

      List<String> newPath = new ArrayList<>(path.subList(0, path.size() - 1));
      newPath.add(String.format(CONNECTOR_PATH_TEMPLATE, expression));

      return newPath;
    }

    return path;
  }
}
