/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.jdbc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Callable;

public class ExceptionHelper<V> {

  private final Exception exception;
  private final V value;

  public ExceptionHelper(Exception exception, final V value) {
    this.exception = exception;
    this.value = value;
  }

  public ExceptionHelper(final Exception ex) {
    this(ex, null);
  }

  public ExceptionHelper(final V value) {
    this(null, value);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> RuntimeException sneakyThrow(final Throwable t) throws T {
    throw (T) t;
  }

  public static void uncheckedRun(final RunnableWithException runnable) {
    try {
      runnable.run();
    } catch (Exception ex) {
      throw sneakyThrow(ex);
    }
  }

  public static <V> V uncheckedCall(final Callable<V> callable) {
    try {
      return callable.call();
    } catch (Exception ex) {
      throw sneakyThrow(ex);
    }
  }

  public static <V> ExceptionHelper<V> call(final Callable<V> callable) {
    try {
      return new ExceptionHelper<V>(callable.call());
    } catch (Exception ex) {
      return new ExceptionHelper<V>(ex);
    }
  }

  public boolean isOk() {
    return !isErr();
  }

  public boolean isErr() {
    return exception != null;
  }

  public V unwrap() {
    if (isErr()) {
      throw new IllegalStateException("get value when result type is error");
    }
    return value;
  }

  public Exception unwrapErr() {
    if (isErr()) {
      return exception;
    }
    throw new IllegalStateException("get error when result type is ok");
  }

  public static String stringify(Throwable throwable) {
    final StringWriter writer = new StringWriter();
    final PrintWriter printer = new PrintWriter(writer);
    throwable.printStackTrace(printer);
    return writer.toString();
  }

  @FunctionalInterface
  public interface RunnableWithException {

    void run() throws Exception;
  }
}
