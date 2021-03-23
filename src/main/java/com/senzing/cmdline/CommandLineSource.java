package com.senzing.cmdline;

/**
 * Describes the source of a {@link CommandLineValue}.
 */
public enum CommandLineSource {
  /**
   * The value comes from the default value associated with the option.
   */
  DEFAULT,

  /**
   * The value comes from an environment variable.
   */
  ENVIRONMENT,

  /**
   * The value comes from a command-line flag.
   */
  COMMAND_LINE;
}
