package com.senzing.api.model;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The response containing a set of data source codes.  Typically this is the
 * list of all configured data source codes.
 *
 */
public class SzDataSourcesResponse extends SzResponseWithRawData
{
  /**
   * The set of data source codes.
   */
  private Set<String> dataSources;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * license info data to be initialized later.
   *
   * @param method The {@link SzHttpMethod}.
   * @param selfLink The string URL link to generate this response.
   */
  public SzDataSourcesResponse(SzHttpMethod method,
                               String       selfLink) {
    super(method, selfLink);
    this.dataSources = new LinkedHashSet<>();
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
   * Adds the specified data source providing the specified data source
   * is not already containe
   *
   * @param dataSource The data source code to add.
   */
  public void addDataSource(String dataSource) {
    dataSource = dataSource.trim().toUpperCase();
    if (this.dataSources.contains(dataSource)) return;
    this.dataSources.add(dataSource);
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
     * Gets the {@link Set} of
     * @return
     */
    public Set<String> getDataSources() {
      Set<String> set = SzDataSourcesResponse.this.dataSources;
      return Collections.unmodifiableSet(set);
    }
  }

}
