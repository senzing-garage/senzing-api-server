package com.senzing.api.model.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.senzing.api.model.*;

import java.util.*;

/**
 * The response containing a set of entity class codes.  Typically this is the
 * list of all configured entity class codes.
 *
 */
@JsonDeserialize
public class SzEntityClassesResponseImpl extends SzResponseWithRawDataImpl
  implements SzEntityClassesResponse
{
  /**
   * The data for this instance.
   */
  private SzEntityClassesResponseData data = null;

  /**
   * Package-private default constructor.
   */
  protected SzEntityClassesResponseImpl() {
    this.data = null;
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity classs to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityClassesResponseImpl(SzMeta meta, SzLinks links) {
    this(meta, links, SzEntityClassesResponseData.FACTORY.create());
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * entity classs to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzEntityClassesResponseImpl(SzMeta                       meta,
                                     SzLinks                      links,
                                     SzEntityClassesResponseData  data)
  {
    super(meta, links);
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SzEntityClassesResponseData getData() {
    return this.data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setData(SzEntityClassesResponseData data) {
    this.data = data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEntityClass(SzEntityClass entityClass) {
    this.data.addEntityClass(entityClass);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEntityClasses(
      Collection<? extends SzEntityClass> entityClasses)
  {
    this.data.setEntityClasses(entityClasses);
  }
}
