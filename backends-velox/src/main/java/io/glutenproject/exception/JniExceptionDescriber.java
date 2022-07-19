package io.glutenproject.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

public class JniExceptionDescriber {
  private JniExceptionDescriber() {
  }

  static String describe(Throwable throwable) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.getBuffer().toString();
  }
}
