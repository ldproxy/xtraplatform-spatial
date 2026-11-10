/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.entities.domain.ValidationResult;
import de.ii.xtraplatform.entities.domain.ValidationResult.MODE;
import java.util.List;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface SourceSchemaValidator<T extends SchemaBase<T>> {

  ValidationResult validate(String typeName, List<T> sourceSchemas, MODE mode);
}
