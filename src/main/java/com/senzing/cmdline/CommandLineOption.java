package com.senzing.cmdline;

import java.util.Collections;
import java.util.Set;

/**
 * Interface for {@link Enum} classes that represent command line options
 * to enable them to beused with {@link CommandLineUtilities}.
 *
 * @param <T>
 */
public interface CommandLineOption<T extends Enum<T>> {
  /**
   * Gets the {@link String} command line flag associated with this option.
   *
   * @return The {@link String} command line flag associated with this option.
   */
  String getCommandLineFlag();

  /**
   * Gets an {@link Set} of options that conflict with this option.
   *
   * @return An {@link Set} of options that conflict with this option.
   */
  default Set<T> getConflicts() {
    return Collections.emptySet();
  }

  /**
   * Gets the {@link Set} of {@link Set} instances describing combinations
   * of sets of options that this option depends on.  At least one of the
   * contained {@link Set} instances must be satisfied for this option to
   * be used.
   *
   * @return The {@link Set} of {@link Set} instances describing
   *         combinations of sets of options that this option depends on.
   */
  default Set<Set<T>> getDependencies() {
    return Collections.emptySet();
  }

  /**
   * Returns the {@link Set} of alternative options to use in place of a
   * deprecated option.
   *
   * @return The {@link Set} of alternative options to use in place of a
   *         deprecated option.
   */
  default Set<T> getDeprecationAlternatives() {
    return Collections.emptySet();
  }

  /**
   * Checks if this command line option is a primary option.  At least one
   * primary option must be specified.  Whether or not multiple primary options
   * are allowed depends on the {@linkplain #getConflicts() conflicts} for
   * each option.
   *
   * @return <tt>true</tt> if this option is a primary option, otherwise
   *         <tt>false</tt>.
   */
  default boolean isPrimary() {
    return false;
  }

  /**
   * Checks if this command line option is deprecated.
   *
   * @return <tt>true</tt> if this option is deprecated, otherwise
   *         <tt>false</tt>.
   */
  default boolean isDeprecated() {
    return false;
  }

  /**
   * Returns the minimum number of additional parameters that should follow this
   * command line option.  This should return a non-negative number and at least
   * this mean parameters will be consumed.  By default this returns zero (0).
   */
  default int getMinimumParameterCount() {
    return 0;
  }

  /**
   * Returns the maximum number of additional parameters that should follow this
   * command line option.  If this returns a negative number then parameters
   * are read until the next recognized command line option is read after
   * consuming at least the {@linkplain #getMinimumParameterCount()} minimum
   * number of parameters}.  By default this returns negative one (-1).
   */
  default int getMaximumParameterCount() {
    return -1;
  }
}
