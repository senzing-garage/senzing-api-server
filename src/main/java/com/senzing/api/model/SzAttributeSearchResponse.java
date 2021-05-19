package com.senzing.api.model;

import java.util.*;

/**
 * The response containing a list of {@link SzAttributeSearchResult} instances
 * describing the search results.
 *
 */
public class SzAttributeSearchResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzAttributeSearchResponse() {
    this.data.searchResults = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   */
  public SzAttributeSearchResponse(SzMeta meta, SzLinks links)
  {
    super(meta, links);
    this.data.searchResults = new LinkedList<>();
  }

  /**
   * Returns the {@link Data} for this instance.
   *
   * @return The {@link Data} for this instance.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Sets the {@link List} of {@link SzAttributeSearchResult} instances to the
   * specified list of results.
   *
   * @param results The {@link List} of {@link SzAttributeSearchResult} results.
   */
  public void setSearchResults(List<SzAttributeSearchResult> results)
  {
    this.data.searchResults.clear();
    if (results != null) {
      this.data.searchResults.addAll(results);
    }
  }

  /**
   * Adds the specified {@link SzAttributeSearchResult} to the list of results.
   *
   * @param result The {@link SzAttributeSearchResult} result to add.
   */
  public void addSearchResult(SzAttributeSearchResult result) {
    this.data.searchResults.add(result);
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The list of {@link SzAttributeSearchResult} instances describing the
     * results.
     */
    private List<SzAttributeSearchResult> searchResults;

    /**
     * Private default constructor.
     */
    private Data() {
      // do nothing
    }

    /**
     * Gets the {@link List} of {@linkplain SzAttributeSearchResult search
     * results}.
     *
     * @return {@link List} of {@linkplain SzAttributeSearchResult search
     *          results}
     */
    public List<SzAttributeSearchResult> getSearchResults() {
      List<SzAttributeSearchResult> list = this.searchResults;
      return Collections.unmodifiableList(list);
    }
  }
}
