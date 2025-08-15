/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.PropertyBase;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import org.immutables.value.Value;

// TODO: delete
@Value.Modifiable
@Value.Style(set = "*")
public interface PropertySql extends PropertyBase<PropertySql, SchemaSql>, ObjectSql {}
