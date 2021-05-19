package com.senzing.cmdline;

import java.util.Map;

/**
 * Creates an instance of the parameterized type using a {@link Map}
 * of {@link CommandLineOption} keys to {@link Object} command-line values.
 */
public interface CommandLineBuilder <T> {
  /**
   * Creates an instance of the parameterized type using the specified
   * command-line options.
   *
   * @param options The {@link Map} of {@link CommandLineOption} keys to
   *                command-line {@link Object} values.
   *
   * @return The create instance.
   *
   * @throws Exception If a failure occurs.
   */
  T build(Map<CommandLineOption, Object> options) throws Exception;
}
