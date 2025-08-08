/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FeatureTokenBuffer<
        U extends SchemaBase<U>, V extends SchemaMappingBase<U>, W extends ModifiableContext<U, V>>
    implements FeatureEventHandler<U, V, W> {

  private final FeatureEventHandler<U, V, W> downstream;
  private final List<Object> buffer;
  private final FeatureEventHandler<U, V, W> bufferIn;
  private final FeatureTokenReader<U, V, W> bufferOut;
  private boolean doBuffer;
  private int mark;

  public FeatureTokenBuffer(FeatureEventHandler<U, V, W> downstream, W context) {
    this.downstream = downstream;
    this.buffer = new ArrayList<>();
    this.bufferIn = (FeatureTokenEmitter2<U, V, W>) (buffer::add);
    this.bufferOut = new FeatureTokenReader<>(downstream, context);
    this.doBuffer = false;
    this.mark = -1;
  }

  public void bufferStart() {
    this.doBuffer = true;
  }

  public void bufferStop(boolean flush) {
    this.doBuffer = false;
    if (flush) {
      bufferFlush();
    }
  }

  public void bufferFlush() {
    buffer.add(FeatureTokenType.FLUSH);
    buffer.forEach(bufferOut::onToken);
    buffer.clear();
    this.mark = -1;
  }

  public void bufferMark() {
    this.mark = buffer.size();
  }

  public void bufferInsert(Object token) {
    if (mark > -1) {
      buffer.add(mark, token);
      this.mark++;
    } else {
      throw new IllegalStateException("no mark set");
    }
  }

  public boolean isBuffering() {
    return doBuffer;
  }

  public Stream<Object> bufferAsStream() {
    return buffer.stream();
  }

  @Override
  public void onStart(W context) {
    if (doBuffer) {
      bufferIn.onStart(context);
    } else {
      downstream.onStart(context);
    }
  }

  @Override
  public void onEnd(W context) {
    if (doBuffer) {
      bufferIn.onEnd(context);
    } else {
      downstream.onEnd(context);
    }
  }

  @Override
  public void onFeatureStart(W context) {
    if (doBuffer) {
      bufferIn.onFeatureStart(context);
    } else {
      downstream.onFeatureStart(context);
    }
  }

  @Override
  public void onFeatureEnd(W context) {
    if (doBuffer) {
      bufferIn.onFeatureEnd(context);
    } else {
      downstream.onFeatureEnd(context);
    }
  }

  @Override
  public void onObjectStart(W context) {
    if (doBuffer) {
      bufferIn.onObjectStart(context);
    } else {
      downstream.onObjectStart(context);
    }
  }

  @Override
  public void onObjectEnd(W context) {
    if (doBuffer) {
      bufferIn.onObjectEnd(context);
    } else {
      downstream.onObjectEnd(context);
    }
  }

  @Override
  public void onArrayStart(W context) {
    if (doBuffer) {
      bufferIn.onArrayStart(context);
    } else {
      downstream.onArrayStart(context);
    }
  }

  @Override
  public void onArrayEnd(W context) {
    if (doBuffer) {
      bufferIn.onArrayEnd(context);
    } else {
      downstream.onArrayEnd(context);
    }
  }

  @Override
  public void onGeometry(W context) {
    if (doBuffer) {
      bufferIn.onGeometry(context);
    } else {
      downstream.onGeometry(context);
    }
  }

  @Override
  public void onValue(W context) {
    if (doBuffer) {
      bufferIn.onValue(context);
    } else {
      downstream.onValue(context);
    }
  }
}
