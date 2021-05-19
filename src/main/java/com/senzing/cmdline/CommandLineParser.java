package com.senzing.cmdline;

import java.util.Map;

/**
 * Provides an interface for parsing the command line.
 */
public interface CommandLineParser {
  /**
   * Implement this method to parse the command line arguments and produce a
   * {@link Map} of {@link CommandLineOption} keys to {@link Object} values.
   *
   * @param args The command-line arguments to parse.
   *
   * @return The {@link Map} of {@link CommandLineOption} keys to {@link Object}
   *         values describing the provided arguments.
   */
  Map<CommandLineOption, Object> parseCommandLine(String[] args);
}
