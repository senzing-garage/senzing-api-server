package com.senzing.cmdline;

import com.senzing.configmgr.ConfigurationManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static com.senzing.util.LoggingUtilities.*;

/**
 * Utility functions for parsing command line arguments.
 *
 */
public class CommandLineUtilities {
  /**
   * The JAR file name containing this class.
   */
  public static final String JAR_FILE_NAME;

  /**
   * The base URL of the JAR file containing this class.
   */
  public static final String JAR_BASE_URL;

  /**
   * The URL path to the JAR file containing this class.
   */
  public static final String PATH_TO_JAR;

  static {
    String jarBaseUrl   = null;
    String jarFileName  = null;
    String pathToJar    = null;

    try {
      Class<CommandLineUtilities> cls = CommandLineUtilities.class;

      String url = cls.getResource(
          cls.getSimpleName() + ".class").toString();

      if (url.indexOf(".jar") >= 0) {
        int index = url.lastIndexOf(
            cls.getName().replace(".", "/") + ".class");
        jarBaseUrl = url.substring(0, index);

        index = jarBaseUrl.lastIndexOf("!");
        if (index >= 0) {
          url = url.substring(0, index);
          index = url.lastIndexOf("/");

          if (index >= 0) {
            jarFileName = url.substring(index + 1);
          }

          url = url.substring(0, index);
          index = url.indexOf("/");
          pathToJar = url.substring(index);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      JAR_BASE_URL  = jarBaseUrl;
      JAR_FILE_NAME = jarFileName;
      PATH_TO_JAR   = pathToJar;
    }
  }

  /**
   * Private default constructor.
   */
  private CommandLineUtilities() {
    // do nothing
  }

  /**
   * Utility method to ensure a command line argument with the specified index
   * exists and if not then throws an exception.
   *
   * @param args  The array of command line arguments.
   * @param index The index to check.
   * @throws IllegalArgumentException If the argument does not exist.
   */
  public static void ensureArgument(String[] args, int index) {
    if (index >= args.length) {
      String msg = "Missing expected argument following " + args[index - 1];

      System.err.println();
      System.err.println(msg);

      setLastLoggedAndThrow(new IllegalArgumentException(msg));
    }
  }

  /**
   * Returns a new {@link String} array that contains the same elements as
   * the specified array except for the first N arguments where N is the
   * specified count.
   *
   * @param args  The array of command line arguments.
   *
   * @param count The number of arguments to shift.
   *
   * @return The shifted argument array.
   *
   * @throws IllegalArgumentException If the specified count is negative.
   */
  public static String[] shiftArguments(String[] args, int count)
  {
    if (count < 0) {
      throw new IllegalArgumentException(
          "The specified count cannot be negative: " + count);
    }
    String[] args2 = new String[args.length - count];
    for (int index = 0; index < args2.length; index++) {
      args2[index] = args[index + count];
    }
    return args2;
  }

  /**
   * Stores the option and its value(s) in the specified option map, first
   * checking to ensure that the option is NOT already specified and
   * checking if the option has any conflicts.  If the specified option is
   * deprecated then a warning message is returned.  If only a single option
   * is specified then it is placed as the value.  If more than one option is
   * specified then the value in the map is an array of the specified values.
   *
   * @param optionMap The {@link Map} to put the option in.
   *
   * @param option The option to use as a key.
   *
   * @param values The one or more values to associate with the option.
   */
  public static <T extends CommandLineOption> void putOption(
      Map<T, Object> optionMap, T option, Object... values)
  {
    Set<T> optionKeys = optionMap.keySet();
    if (optionMap.containsKey(option)) {
      String msg = "Cannot specify command-line option more than once: "
          + option.getCommandLineFlag();

      System.err.println();
      System.err.println(msg);

      setLastLoggedAndThrow(new IllegalArgumentException(msg));
    }

    for (T opt : optionKeys) {
      Set<T> conflicts = option.getConflicts();
      if (conflicts != null && conflicts.contains(opt)) {
        String msg = "Cannot specify both the " + opt.getCommandLineFlag()
            + " and " + option.getCommandLineFlag() + " options.";

        System.err.println();
        System.err.println(msg);

        setLastLoggedAndThrow(new IllegalArgumentException(msg));
      }
    }

    // check for deprecation
    if (option.isDeprecated()) {
      System.err.println();
      System.err.println("WARNING: The " + option.getCommandLineFlag()
                         + " option is deprecated and will be removed in a "
                         + "future release.");
      Set<T> alternatives = option.getDeprecationAlternatives();
      if (alternatives.size() == 1) {
        T alternative = alternatives.iterator().next();
        System.err.println();
        System.err.println("Consider using " + alternative.getCommandLineFlag()
                           + " instead.");
      } else if (alternatives.size() > 1) {
        System.err.println();
        System.err.println("Consider using one of the following instead:");
        for (T alternative : alternatives) {
          System.err.println("     " + alternative.getCommandLineFlag());
        }
      }
      System.err.println();
    }

    // put it in the option map
    if (values != null && values.length == 1) {
      optionMap.put(option, values[0]);
    } else {
      optionMap.put(option, values);
    }
  }

  /**
   * Returns the enumerated {@link CommandLineOption} value associated with
   * the specified command line flag and enumerated {@link CommandLineOption}
   * class.
   *
   * @param enumClass The {@link Class} for the {@link CommandLineOption}
   *                  implementation.
   *
   * @param commandLineFlag The command line flag.
   *
   * @return The enumerated {@link CommandLineOption} or <tt>null</tt> if not
   *         found.
   */
  public static <T extends Enum<T> & CommandLineOption<T>> T lookup(
      Class<T> enumClass,
      String   commandLineFlag)
  {
    // just iterate to find it rather than using a lookup map given that
    // enums are usually not more than a handful of values
    EnumSet<T> enumSet = EnumSet.allOf(enumClass);
    for (T enumVal : enumSet) {
      if (enumVal.getCommandLineFlag().equals(commandLineFlag)) {
        return enumVal;
      }
    }
    return null;
  }

  /**
   * Validates the specified {@link Set} of specified {@link CommandLineOption}
   * instances and ensures that they logically make sense together.  This
   * checks for the existing of at least one primary option (if primary options
   * exist), ensures there are no conflicts and that all dependencies are
   * satisfied.
   *
   * @throws IllegalArgumentException If the specified options are invalid
   *                                  together.
   */
  public static <T extends Enum<T> & CommandLineOption<T>> void validateOptions(
      Class<T>  enumClass,
      Set<T>    specifiedOptions)
      throws IllegalArgumentException
  {
    // check if we need a primary option
    boolean primaryRequired = false;
    EnumSet<T> enumSet = EnumSet.allOf(enumClass);
    for (T enumVal : enumSet) {
      if (enumVal.isPrimary()) {
        primaryRequired = true;
        break;
      }
    }

    if (primaryRequired) {
      // if primary option is required then check for at least one
      int primaryCount = 0;
      for (T option : specifiedOptions) {
        if (option.isPrimary()) {
          primaryCount++;
        }
      }
      if (primaryCount == 0) {
        StringWriter sw = new StringWriter();
        PrintWriter  pw = new PrintWriter(sw);

        pw.println("Must specify at least one of the following:");
        for (T option : enumSet) {
          if (option.isPrimary()) {
            pw.println("     " + option.getCommandLineFlag()
                       + (option.isDeprecated() ? " (deprecated)" : ""));
          }
        }

        String msg = sw.toString();
        pw.flush();
        System.err.println();
        System.err.println(msg);
        setLastLoggedAndThrow(new IllegalArgumentException(msg));
      }
    }

    // check for conflicts and dependencies
    for (T option : specifiedOptions) {
      Set<T>      conflicts     = option.getConflicts();
      Set<Set<T>> dependencies  = option.getDependencies();
      if (conflicts != null) {
        for (T conflict : conflicts) {
          // skip if the same option -- cannot conflict with itself
          if (option == conflict) continue;

          // check if the conflict is present
          if (specifiedOptions.contains(conflict)) {
            String msg = "Cannot specify both " + option.getCommandLineFlag()
                + " and " + conflict.getCommandLineFlag();
            System.err.println();
            System.err.println(msg);

            setLastLoggedAndThrow(new IllegalArgumentException(msg));
          }
        }
      }
      boolean satisfied = (dependencies == null || dependencies.size() == 0);
      if (!satisfied) {
        for (Set<T> dependencySet : dependencies) {
          if (specifiedOptions.containsAll(dependencySet)) {
            satisfied = true;
            break;
          }
        }
      }
      if (!satisfied) {
        System.err.println();
        if (dependencies.size() == 1) {
          System.err.println(
              "The " + option.getCommandLineFlag() + " option also requires:");
          Set<T> dependencySet = dependencies.iterator().next();
          for (T dependency : dependencySet) {
            if (!specifiedOptions.contains(dependency)) {
              System.err.println("     " + dependency.getCommandLineFlag());
            }
          }
        } else {
          System.err.println(
              "The " + option.getCommandLineFlag() + " option also requires:");
          String leader = "     ";
          for (Set<T> dependencySet : dependencies) {
            String  prefix      = "";
            String  prevOption  = null;
            System.err.print(leader);
            leader = "  or ";
            for (T dependency : dependencySet) {
              int     count       = 0;
              if (!specifiedOptions.contains(dependency)) {
                if (prevOption != null) {
                  count++;
                  System.err.print(prefix + prevOption);
                }
                prevOption = dependency.getCommandLineFlag();
                prefix = ", ";
              }
              if (count > 0) {
                System.err.print(" and ");
              }
              System.err.println(prevOption);
            }
          }
        }
        setLastLoggedAndThrow(new IllegalArgumentException(
            "Missing dependencies for " + option.getCommandLineFlag()));
      }
    }
  }

  /**
   * Parses the command line arguments and returns a {@link Map} of those
   * arguments.
   *
   * @param enumClass The enumerated {@link CommandLineOption} class.
   * @param args The arguments to parse.
   * @return A {@link Map} of {@link CommandLineOption} keys to
   */
  public static <T extends Enum<T> & CommandLineOption<T>> Map<T, Object>
    parseCommandLine(Class<T>               enumClass,
                     String[]               args,
                     ParameterProcessor<T>  processor)
  {
    EnumSet<T> enumSet = EnumSet.allOf(enumClass);
    Map<String, T> lookupMap = new LinkedHashMap<>();
    for (T option : enumSet) {
      lookupMap.put(option.getCommandLineFlag(), option);
    }
    Map<T, Object> result = new LinkedHashMap<>();
    for (int index = 0; index < args.length; index++) {
      T option = lookupMap.get(args[index]);
      if (option == null) {
        System.err.println();
        System.err.println("Unrecognized option: " + args[index]);

        setLastLoggedAndThrow(new IllegalArgumentException(
            "Unrecognized command line option: " + args[index]));
      }
      int minParamCount = option.getMinimumParameterCount();
      int maxParamCount = option.getMaximumParameterCount();
      if (maxParamCount > 0 && maxParamCount < minParamCount) {
        throw new IllegalStateException(
            "The non-negative maximum parameter count is less than the minimum "
            + "parameter count.  min=[ " + minParamCount + " ], max=[ "
            + maxParamCount + " ], option=[ " + option + " ]");
      }
      List<String> params = new ArrayList<>(maxParamCount<0 ? 5:maxParamCount);
      if (minParamCount > 0) {
        // check if there are enough parameters
        int max = index + minParamCount;
        for (index++; index <= max; index++) {
          ensureArgument(args, index);
          params.add(args[index]);
        }
        index--;
      }
      int bound = (maxParamCount < 0)
                ? args.length
                : index + (maxParamCount - minParamCount) + 1;
      if (bound > args.length) bound = args.length;
      for (int nextIndex = index + 1;
           nextIndex < bound && !lookupMap.containsKey(args[nextIndex]);
           nextIndex++)
      {
        params.add(args[nextIndex]);
        index++;
      }

      // process the parameters
      if (processor != null) {
        Object value = null;
        try {
          value = processor.process(option, params);
        } catch (Exception e) {
          System.err.println();
          System.err.println("Bad parameters for " + option.getCommandLineFlag() + " option:");
          for (String p : params) {
            System.err.println("    o " + p);
          }
          System.err.println();
          System.err.println(e.getMessage());
          if (e instanceof RuntimeException) throw (RuntimeException) e;
          throw new RuntimeException(e);
        }
        putOption(result, option, value);

      } else if (params.size() == 1) {
        putOption(result, option, null);
      } else if (params.size() > 1) {
        putOption(result, option, params.toArray());
      }
    }

    validateOptions(enumClass, result.keySet());
    return result;
  }

  /**
   * Checks if the specified {@link Class} was the class whose static
   * <tt>main(String[])</tt> function was called to begin execution of the
   * current process.
   *
   * @param cls The {@link Class} to test for.
   *
   * @return <tt>true</tt> if the specified class' static
   *         <tt>main(String[])</tt> function was called to begin execution of
   *         the current process.
   */
  public static boolean checkClassIsMain(Class cls) {
    // check if called from the ConfigurationManager.main() directly
    Throwable t = new Throwable();
    StackTraceElement[] trace = t.getStackTrace();
    StackTraceElement lastStackFrame = trace[trace.length-1];
    String className = lastStackFrame.getClassName();
    String methodName = lastStackFrame.getMethodName();
    return ("main".equals(methodName) && cls.getName().equals(className));
  }

  /**
   * Returns a multi-line bulleted list of the specified options with the
   * specified indentation.
   *
   * @param indent The number of spaces to indent.
   * @param options The zero or more options to write.
   *
   * @return The multi-line bulleted list of options.
   */
  public static String formatUsageOptionsList(int                  indent,
                                              CommandLineOption... options)
  {
    StringWriter sw = new StringWriter();
    PrintWriter  pw = new PrintWriter(sw);
    int maxLength = 0;
    StringBuilder sb = new StringBuilder();
    for (int index = 0; index < indent; index++) {
      sb.append(" ");
    }
    String indenter = sb.toString();

    for (CommandLineOption option : options) {
      if (option.getCommandLineFlag().length() > maxLength) {
        maxLength = option.getCommandLineFlag().length();
      }
    }
    String spacer = "  ";
    String bullet = "o ";
    maxLength += (bullet.length() + spacer.length());
    // check how many options per line we can fit
    int columnCount = (80 - indent - spacer.length()) / maxLength;

    // check if we can balance things out if we have a single dangling option
    if (options.length == columnCount + 1) {
        columnCount--;
    }

    // if less than 1, then set a minimum of 1 column
    if (columnCount < 1) columnCount = 1;

    int columnIndex = 0;
    for (CommandLineOption option: options) {
      String flag       = option.getCommandLineFlag();
      int    spaceCount = maxLength - flag.length() - bullet.length();
      if (columnIndex == 0) pw.print(indenter);
      else pw.print(spacer);
      pw.print(bullet);
      pw.print(flag);
      for (int index = 0; index < spaceCount; index++) {
        pw.print(" ");
      }
      if (columnIndex == columnCount - 1) {
        pw.println();
        columnIndex = 0;
      } else {
        columnIndex++;
      }
    }
    if (columnIndex != 0) {
      pw.println();
    }
    pw.flush();
    return sw.toString();
  }
}
