package com.senzing.cmdline;

import java.util.List;

/**
 * Represents a value obtained from the command-line parser for a given
 * {@link CommandLineOption}.
 */
public class CommandLineValue {
  /**
   * The option for this value.
   */
  private CommandLineOption option;

  /**
   * The {@link CommandLineSource} from whence the value came.
   */
  private CommandLineSource source;

  /**
   * The command-line flag or environment variable from which the value
   * was obtained if the source is not {@link CommandLineSource#DEFAULT}.
   */
  private String specifier;

  /**
   * The processed value for the option.
   */
  private Object processedValue;

  /**
   * The original text parameter values for the option.
   */
  private List<String> parameters;

  /**
   * Constructs with the specified option, {@link CommandLineSource}, processed
   * value and {@link List} of {@link String} parameters that were used to
   * specify this option.
   *
   * @param option The {@link CommandLineOption} that for with this value.
   * @param source The {@link CommandLineSource} that represents the source of
   *               the value.
   * @param processedValue The processed value from the command-line parser.
   * @param parameters The {@link String} parameters used to specify this.
   */
  public CommandLineValue(CommandLineOption option,
                          CommandLineSource source,
                          Object            processedValue,
                          List<String>      parameters)
  {
    this(option, source, null, processedValue, parameters);
  }

  /**
   * Constructs with the specified option, {@link CommandLineSource}, processed
   * value and {@link List} of {@link String} parameters that were used to
   * specify this option.
   *
   * @param option The {@link CommandLineOption} that for with this value.
   * @param source The {@link CommandLineSource} that represents the source of
   *               the value.
   * @param specifier The {@link String} specifier associated with the {@link
   *                  CommandLineSource} or <tt>null</tt> if it does not apply.
   * @param processedValue The processed value from the command-line parser.
   * @param parameters The {@link String} parameters used to specify this.
   */
  public CommandLineValue(CommandLineOption option,
                          CommandLineSource source,
                          String            specifier,
                          Object            processedValue,
                          List<String>      parameters)
  {
    this.option         = option;
    this.source         = source;
    this.specifier      = specifier;
    this.processedValue = processedValue;
    this.parameters     = List.copyOf(parameters);
  }

  /**
   * Gets the associated {@link CommandLineOption}.
   *
   * @return The associated {@link CommandLineOption}.
   */
  public CommandLineOption getOption() {
    return this.option;
  }

  /**
   * Gets the associated {@link CommandLineSource}.
   *
   * @return The associated {@link CommandLineSource}.
   */
  public CommandLineSource getSource() {
    return this.source;
  }

  /**
   * Sets the associated {@link CommandLineSource}.
   *
   * @param source The associated {@link CommandLineSource}.
   */
  public void setSource(CommandLineSource source) {
    this.source = source;
  }

  /**
   * Gets the specifier associated with the {@link CommandLineSource} (if any).
   * This returns <tt>null</tt> if none.
   *
   * @return The specifier associated with the {@link CommandLineSource}, or
   *         <tt>null</tt> if none.
   */
  public String getSpecifier() {
    return this.specifier;
  }

  /**
   * Sets the specifier associated with the {@link CommandLineSource} (if any).
   * Set this to <tt>null</tt> if none.
   *
   * @param specifier The specifier associated with the {@link
   *                  CommandLineSource}, or <tt>null</tt> if none.
   */
  public void setSpecifier(String specifier) {
    this.specifier = specifier;
  }

  /**
   * Gets the processed value associated with the {@link CommandLineOption}.
   *
   * @return The processed value associated with the {@link CommandLineOption}.
   */
  public Object getProcessedValue() {
    return this.processedValue;
  }

  /**
   * Sets the processed value associated with the {@link CommandLineOption}.
   *
   * @param processedValue The processed value associated with the {@link
   *                       CommandLineOption}.
   */
  public void setProcessedValue(Object processedValue) {
    this.processedValue = processedValue;
  }

  /**
   * Gets the {@link List} of {@link String} parameters for this option.
   * These are the unprocessed parameter values that were passed.
   *
   * @return The {@link List} of {@link String} parameters for this option.
   */
  public List<String> getParameters() {
    return this.parameters;
  }

  /**
   * Sets the {@link List} of {@link String} parameters for this option.
   * These are the unprocessed parameter values that were passed.
   *
   * @param parameters The {@link List} of {@link String} parameters for this
   *                   option.
   */
  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }
}
