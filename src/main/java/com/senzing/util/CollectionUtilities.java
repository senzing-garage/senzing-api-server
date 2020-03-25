package com.senzing.util;

import java.util.*;

public class CollectionUtilities {
  /**
   * Private default constructor.
   */
  private CollectionUtilities() {
    //
  }

  /**
   * Returns a recursively unmodifiable view of the specified {@link List}.
   * The behavior of this recursion depends on the values of the specified
   * {@link List}:
   * <ul>
   *   <li>{@link Set} - Uses {@link Collections#unmodifiableSet(Set)}
   *       to make the {@link Set} unmodifiable, but does not recurse further
   *       because the elements of a {@link Set} cannot be modified in place.
   *   </li>
   *   <li>{@link List} - Recursively calls {@link
   *       #recursivelyUnmodifiableList(List)} on the {@link List} value to
   *       make it unmodifiable.
   *   </li>
   *   <li>{@link Map} - Calls {@link #recursivelyUnmodifiableMap(Map)} on the
   *       {@link Map} value to make it unmodifiable.
   *   </li>
   *   <li>All other values (including <tt>null</tt> values) are left as-is</li>
   * </ul>
   *
   * @param list The {@link List} to make recursively undmodifiable.
   * @return The recursively unmodifiable view of the specified {@link List}.
   */
  public static <T> List<T> recursivelyUnmodifiableList(List<T> list) {
    ListIterator<T> iter = list.listIterator();
    while (iter.hasNext()) {
      T elem = iter.next();
      if (elem instanceof Set) {
        iter.set((T) Collections.unmodifiableSet((Set) elem));
      } else if (elem instanceof List) {
        iter.set((T) recursivelyUnmodifiableList((List) elem));
      } else if (elem instanceof Map) {
        iter.set((T) recursivelyUnmodifiableMap((Map) elem));
      }
    }
    return Collections.unmodifiableList(list);
  }

  /**
   * Returns a recursively unmodifiable view of the specified {@link Map}.
   * The keys of the {@link Map} are left unmodifiable, but the behavior of this
   * recursion depends on the values of the specified {@link Map}:
   * <ul>
   *   <li>{@link Set} - Uses {@link Collections#unmodifiableSet(Set)}
   *       to make the {@link Set} unmodifiable, but does not recurse further
   *       because the elements of a {@link Set} cannot be modified in place.
   *   </li>
   *   <li>{@link List} - Calls {@link #recursivelyUnmodifiableList(List)} on
   *       the {@link List} value to make it unmodifiable.
   *   </li>
   *   <li>{@link Map} - Recursively calls {@link
   *       #recursivelyUnmodifiableMap(Map)} on the {@link Map} value to make
   *       it unmodifiable.
   *   </li>
   *   <li>All other values (including <tt>null</tt> values) are left as-is</li>
   * </ul>
   *
   * @param map The {@link Map} to make recursively undmodifiable.
   * @return The recursively unmodifiable view of the specified {@link List}.
   */
  public static <K, V> Map<K,V> recursivelyUnmodifiableMap(Map<K,V> map) {
    Iterator<Map.Entry<K,V>> iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<K,V> entry = iter.next();
      V value = entry.getValue();
      if (value != null) {
        if (value instanceof Set) {
          entry.setValue((V) Collections.unmodifiableSet((Set) value));
        } else if (value instanceof List) {
          entry.setValue((V) recursivelyUnmodifiableList((List) value));
        } else if (value instanceof Map) {
          entry.setValue((V) recursivelyUnmodifiableMap((Map) value));
        }
      }
    }
    return Collections.unmodifiableMap(map);
  }

  /**
   * Utility method for creating a {@link List} to use in validation.
   *
   * @param elements The zero or more elements in the list.
   *
   * @return The {@link Set} of elements.
   */
  public static <T> List<T> list(T... elements) {
    List<T> list = new ArrayList<>(elements.length);
    for (T element : elements) {
      list.add(element);
    }
    return list;
  }

  /**
   * Utility method for creating a {@link Set} to use in validation.
   *
   * @param elements The zero or more elements in the set.
   *
   * @return The {@link Set} of elements.
   */
  public static <T> Set<T> set(T... elements) {
    Set<T> set = new LinkedHashSet<>();
    for (T element : elements) {
      set.add(element);
    }
    return set;
  }


}
