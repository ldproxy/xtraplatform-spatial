/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenContextSimple;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenTransformerBaseSimple;
import java.util.Objects;

public abstract class FeatureTokenTransformerSql
    extends FeatureTokenTransformerBaseSimple<
        SqlQuerySchema, SqlQueryMapping, ModifiableContext<SqlQuerySchema, SqlQueryMapping>> {

  private ModifiableContext<SqlQuerySchema, SqlQueryMapping> context;

  @Override
  public Class<? extends ModifiableContext<SqlQuerySchema, SqlQueryMapping>> getContextInterface() {
    if (getDownstream() instanceof FeatureTokenContextSimple<?>) {
      return ((FeatureTokenContextSimple<ModifiableContext<SqlQuerySchema, SqlQueryMapping>>)
              getDownstream())
          .getContextInterface();
    }

    return null;
  }

  @Override
  public final ModifiableContext<SqlQuerySchema, SqlQueryMapping> createContext() {
    ModifiableContext<SqlQuerySchema, SqlQueryMapping> context =
        getDownstream() instanceof FeatureTokenContextSimple<?>
            ? ((FeatureTokenContextSimple<ModifiableContext<SqlQuerySchema, SqlQueryMapping>>)
                    getDownstream())
                .createContext()
            : null;

    if (Objects.isNull(this.context)) {
      this.context = context;
    }

    return context;
  }

  protected final ModifiableContext<SqlQuerySchema, SqlQueryMapping> getContext() {
    if (Objects.isNull(context)) {
      return createContext();
    }

    return context;
  }
}
