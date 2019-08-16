package com.senzing.cmdline;

import java.util.List;

public interface ParameterProcessor<T extends Enum<T> & CommandLineOption<T>>
{
  /**
   * Converts the specified command-line {@link String} parameters to a
   * {@link CommandLineOption} into an {@link Object} or array of
   * {@link Object} instances and determines if the parameters appear valid.
   *
   * @param commandLineOption The {@link CommandLineOption} asssociated with
   *                          the parameters.
   *
   * @param params The {@link List} of {@link String} parameters.
   *
   * @return The {@link Object} or array of {@link Object} instances
   *         representing the processed parameters.
   *
   * @throws IllegalArgumentException If the parameters are illegal in some
   *                                  way.
   */
  Object process(T commandLineOption, List<String> params)
    throws IllegalArgumentException;
}
