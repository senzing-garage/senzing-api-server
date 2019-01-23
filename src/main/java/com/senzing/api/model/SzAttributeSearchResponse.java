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
   * The list of {@link SzAttributeSearchResult} instances describing the
   * results.
   */
  private List<SzAttributeSearchResult> searchResults;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   */
  public SzAttributeSearchResponse(SzHttpMethod httpMethod,
                                   int          httpStatusCode,
                                   String       selfLink)
  {
    super(httpMethod, httpStatusCode, selfLink);
    this.searchResults = new LinkedList<>();
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
    this.searchResults.clear();
    if (results != null) {
      this.searchResults.addAll(results);
    }
  }

  /**
   * Adds the specified {@link SzAttributeSearchResult} to the list of results.
   *
   * @param result The {@link SzAttributeSearchResult} result to add.
   */
  public void addSearchResult(SzAttributeSearchResult result) {
    this.searchResults.add(result);
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public class Data {
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
      List<SzAttributeSearchResult> list
          = SzAttributeSearchResponse.this.searchResults;
      return Collections.unmodifiableList(list);
    }
  }
}
