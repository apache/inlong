/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.cdc;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Miscellaneous utility functions and types
 */
public final class Misc {

  public static <T> T uncheckedRun(final Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception ex) {
      throw new RuntimeException("Failed to run callable", ex);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<T> uncheckedLoadClass(final String className) {
    return uncheckedRun(() -> (Class<T>) Class.forName(className));
  }

  public static <T> Constructor<T> uncheckedGetConstructor(final Class<T> clazz) {
    return uncheckedRun(clazz::getConstructor);
  }

  public static <T> Method uncheckedGetMethod(final Class<T> clazz, final String name,
      final Class<?>... parameterTypes) {
    return uncheckedRun(() -> clazz.getMethod(name, parameterTypes));
  }
}