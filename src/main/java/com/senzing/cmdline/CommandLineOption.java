package com.senzing.cmdline;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Interface for {@link Enum} classes that represent command line options
 * to enable them to beused with {@link CommandLineUtilities}.
 *
 * @param <T>
 */
public interface CommandLineOption<T extends Enum<T> & CommandLineOption> {
  /**
   * Gets the base {@link CommandLineOption} type that this one extends.  This
   * returns <tt>null</tt> if there is no base type.
   *
   * @return The base {@link CommandLineOption} type that this one extends, or
   *         <tt>null</tt> if there is no base type.
   *
   */
  default <B extends Enum<B> & CommandLineOption<B>> Class<B> getBaseOptionType() {
    return null;
  }

  /**
   * Gets the {@link String} command line flag associated with this option.
   *
   * @return The {@link String} command line flag associated with this option.
   */
  String getCommandLineFlag();

  /**
   * Gets the <b>unmodifiable</b> {@link Set} of {@link String} synonym flags
   * for this option.  These are alternate command-line flags that can be used
   * instead of the primary flag specified by {@link #getCommandLineFlag()}.
   * This is useful when a flag changes and the old version of it is
   * deprecated but still supported.  It is <b>NOT</b> allowed to pass multiple
   * flags for the same option.
   *
   * @return The <b>unmodifiable</b> {@link Set} of {@link String} synonym flags
   *         for this optionl, or an empty set if there are no synonyms.
   */
  default Set<String> getSynonymFlags() {
    return Collections.emptySet();
  }

  /**
   * Gets the environment variable that can be used to set the value for
   * this option, or <tt>null</tt> if the value for this option cannot be set
   * with an environment variable.
   *
   * @return The  environment variable that can be used to set the value for
   *         this option, or <tt>null</tt> if the value for this option cannot
   *         be set with an environment variable.
   */
  default String getEnvironmentVariable() {
    return null;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Set} of {@link String} synonym
   * environment variables for this option.  These are alternate environment
   * variables that can be used instead of the primary environment variable
   * specified by {@link #getEnvironmentVariable()}.  This is useful when an
   * environment variable changes and the old version of it is deprecated but
   * still supported.  Having multiple environment variables set in the
   * environment for this option is allowed, however, they are taken in order
   * of preference (first the primary environment variable and then each of
   * those in returned {@link Set} in order).
   *
   * @return The <b>unmodifiable</b> {@link Set} of {@link String} synonym flags
   *         for this optionl, or an empty set if there are no synonyms.
   *
   */
  default Set<String> getEnvironmentSynonyms() {
    return Collections.emptySet();
  }

  /**
   * Gets the <b>unmodifiable</b> {@link List} of {@link String} fall-back
   * environment variables for this option.  These are environment variables
   * that are only checked if another option is specified that is dependent
   * on this option and this option is missing.  The environment variables
   * in the returned {@link List} are checked in order until the first one is
   * found.  These are essentially environment synonyms that are only
   * conditionally checked until the first is found.
   *
   * @return The ordered {@link List} of {@link String} fall-back environment
   *         variables.
   */
  default List<String> getEnvironmentFallbacks() {
    return Collections.emptyList();
  }

  /**
   * Gets an {@link Set} of options that conflict with this option.
   *
   * @return An {@link Set} of options that conflict with this option.
   */
  default Set<CommandLineOption> getConflicts() {
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
  default Set<Set<CommandLineOption>> getDependencies() {
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
   * this many parameters will be consumed.  By default this returns zero (0).
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


  /**
   * Returns the default parameter values associated with the option.  These are
   * the parameters that are set if none are specified.  If no default
   * parameters then this returns <tt>null</tt>.  An empty {@link List}
   * indicates that the option is specified by default but with no parameters.
   * The default implementation returns <tt>null</tt>.
   *
   * @return The default parameter values associated with the option.
   */
  default List<String> getDefaultParameters() {
    return null;
  }

//  /**
//   * Returns the group name for the option assuming the option belongs to a
//   * group of with other options.  This returns <tt>null</tt> if the option
//   * does not belong to a group.  The default implementation of this method
//   * returns <tt>null</tt>.
//   *
//   * @return The group name for the option, or <tt>null</tt> if the option does
//   *         not belong to a group.
//   */
//  default String getGroupName() {
//    return null;
//  }

//  /**
//   * Assuming the option belongs to a group, this method returns the property
//   * key identifying the option with that group.  This returns <tt>null</tt> if
//   * the option does not belong to a group.  The default implementation of this
//   * method returns <tt>null</tt>.
//   *
//   * @return The property key for this option with its option group, or
//   *         <tt>null</tt> if the option does not belong to an option group.
//   */
//  default String getGroupPropertyKey() {
//    return null;
//  }

//  /**
//   * Assuming this option belongs to a group, this method checks if this option
//   * is optional with the group.  The default implementation of this method
//   * returns <tt>false</tt>.
//   *
//   * @return Whether or not this option is optional within its option group.
//   */
//  default boolean isGroupOptional() {
//    return false;
//  }
}
