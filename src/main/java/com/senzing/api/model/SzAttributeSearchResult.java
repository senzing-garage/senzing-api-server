package com.senzing.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.senzing.util.JsonUtils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.util.*;
import java.util.function.Function;

import static com.senzing.api.model.SzAttributeSearchResultType.*;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.*;

/**
 * Describes a search attribute result which extends the {@link
 * SzBaseRelatedEntity} to add the {@link SzAttributeSearchResultType} and
 * the {@link SzSearchFeatureScore} instances.
 */
public class SzAttributeSearchResult extends SzBaseRelatedEntity {
  /**
   * The search result type.
   */
  private SzAttributeSearchResultType resultType;

  /**
   * The best name score.
   */
  private Integer bestNameScore;

  /**
   * The {@link Map} of {@link String} feature type keys to {@link List}
   * values of {@link SzSearchFeatureScore} instances.
   */
  private Map<String, List<SzSearchFeatureScore>> featureScores;

  /**
   * The {@link Map} of {@link String} feature type keys to <b>unmodifiable</b>
   * {@link List} values of {@link SzSearchFeatureScore} instances.
   */
  private Map<String, List<SzSearchFeatureScore>> featureScoreViews;

  /**
   * The entities related to the resolved entity.
   */
  private List<SzRelatedEntity> relatedEntities;

  /**
   * Default constructor.
   */
  public SzAttributeSearchResult() {
    this.resultType         = null;
    this.bestNameScore      = null;
    this.featureScores      = new LinkedHashMap<>();
    this.featureScoreViews  = new LinkedHashMap<>();
    this.relatedEntities    = new LinkedList<>();
  }

  /**
   * Gets the {@link SzRelationshipType} describing the type of relation.
   *
   * @return The {@link SzRelationshipType} describing the type of relation.
   */
  public SzAttributeSearchResultType getResultType() {
    return this.resultType;
  }

  /**
   * Sets the {@link SzAttributeSearchResultType} describing the type of
   * relation.
   *
   * @param resultType The {@link SzAttributeSearchResultType} describing the
   *                   type of relation.
   */
  public void setResultType(SzAttributeSearchResultType resultType) {
    this.resultType = resultType;
  }

  /**
   * Gets the best name score from the search match.  This is the best of the
   * full name scores and organization name scores.  This is <tt>null</tt> if
   * there are no such name scores.
   *
   * @return The best name score from the search match, or <tt>null</tt> if
   *         no full name or organization scores.
   */
  @JsonInclude(NON_NULL)
  public Integer getBestNameScore() {
    return this.bestNameScore;
  }

  /**
   * Sets the best full name score from the search match.  This is the best of
   * the full name scores and organization name scores.  Set this to
   * <tt>null</tt> if there are no such name scores.
   *
   * @param score The best name score from the search match, or
   *              <tt>null</tt> if no full name or organization name scores.
   */
  public void setBestNameScore(Integer score) {
    this.bestNameScore = score;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Map} of {@link String} feature type
   * keys to <b>unmodifiable</b> {@link List} values containing instances of
   * {@link SzSearchFeatureScore} describing the feature scores for that type.
   *
   * @return The <b>unmodifiable</b> {@link Map} of {@link String} feature type
   *         keys to <b>unmodifiable</b> {@link List} values contianing
   *         instances of {@link SzSearchFeatureScore} describing the feature scores
   *         for that type.
   */
  @JsonInclude(NON_EMPTY)
  public Map<String, List<SzSearchFeatureScore>> getFeatureScores() {
    if (this.featureScoreViews.size() == 0) return null;
    return Collections.unmodifiableMap(this.featureScoreViews);
  }

  /**
   * Adds the specified {@link SzSearchFeatureScore} to this instance.
   *
   * @param featureScore The {@link SzSearchFeatureScore} to add.
   */
  public void addFeatureScore(SzSearchFeatureScore featureScore) {
    String featureType = featureScore.getFeatureType();
    List<SzSearchFeatureScore> list = this.featureScores.get(featureType);

    // check if the list does not exist
    if (list == null) {
      list = new LinkedList<>();
      List<SzSearchFeatureScore> listView = Collections.unmodifiableList(list);

      this.featureScores.put(featureType, list);
      this.featureScoreViews.put(featureType, listView);
    }

    // add to the list
    list.add(featureScore);
  }

  /**
   * Private setter for JSON marshalling.  The specified {@link Map} will be
   * copied.
   *
   * @param featureScores The {@link Map} of {@link String} feature type
   *                      keys to {@link SzSearchFeatureScore} values.
   */
  private void setFeatureScores(Map<String, List<SzSearchFeatureScore>> featureScores)
  {
    this.featureScores.clear();
    this.featureScoreViews.clear();
    if (featureScores == null) return;
    featureScores.entrySet().forEach(entry -> {
      String                      featureType = entry.getKey();
      List<SzSearchFeatureScore>  list        = entry.getValue();

      List<SzSearchFeatureScore> listCopy = new LinkedList<>();
      List<SzSearchFeatureScore> listView
          = Collections.unmodifiableList(listCopy);
      listCopy.addAll(list);

      this.featureScores.put(featureType, listCopy);
      this.featureScoreViews.put(featureType, listView);
    });
  }

  /**
   * Gets the {@link List} of {@linkplain SzRelatedEntity related entities}.
   *
   * @return The {@link List} of {@linkplain SzRelatedEntity related entities}.
   */
  @JsonInclude(NON_EMPTY)
  public List<SzRelatedEntity> getRelatedEntities() {
    return this.relatedEntities;
  }

  /**
   * Sets the {@link List} of {@linkplain SzRelatedEntity related entities}.
   *
   * @param relatedEntities The {@link List} of {@linkplain SzRelatedEntity
   *                        related entities}.
   */
  public void setRelatedEntities(List<SzRelatedEntity> relatedEntities) {
    this.relatedEntities.clear();
    if (relatedEntities != null) {
      this.relatedEntities.addAll(relatedEntities);
    }
  }

  /**
   * Adds the specified {@link SzRelatedEntity}
   */
  public void addRelatedEntity(SzRelatedEntity relatedEntity) {
    if (relatedEntity != null) {
      this.relatedEntities.add(relatedEntity);
    }
  }

  /**
   * Parses a list of resolved entities from a {@link JsonArray} describing a
   * JSON array in the Senzing native API format for entity features and
   * populates the specified {@link List} or creates a new {@link List}.
   *
   * @param list The {@link List} of {@link SzAttributeSearchResult} instances to
   *             populate, or <tt>null</tt> if a new {@link List}
   *             should be created.
   *
   * @param jsonArray The {@link JsonArray} describing the JSON in the
   *                  Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link List} of {@link
   *         SzAttributeSearchResult} instances.
   */
  public static List<SzAttributeSearchResult> parseSearchResultList(
      List<SzAttributeSearchResult> list,
      JsonArray                     jsonArray,
      Function<String,String>       featureToAttrClassMapper)
  {
    if (list == null) {
      list = new ArrayList<>(jsonArray.size());
    }
    for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
      list.add(parseSearchResult(null,
                                 jsonObject,
                                 featureToAttrClassMapper));
    }
    return list;
  }

  /**
   * Parses the entity feature from a {@link JsonObject} describing JSON
   * for the Senzing native API format for an entity feature and populates
   * the specified {@link SzAttributeSearchResult} or creates a new instance.
   *
   * @param searchResult The {@link SzAttributeSearchResult} instance to
   *                     populate, or <tt>null</tt> if a new instance should
   *                     be created.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON in the
   *                   Senzing native API format.
   *
   * @param featureToAttrClassMapper Mapping function to map feature names to
   *                                 attribute classes.
   *
   * @return The populated (or created) {@link SzAttributeSearchResult}.
   */
  public static SzAttributeSearchResult parseSearchResult(
      SzAttributeSearchResult searchResult,
      JsonObject              jsonObject,
      Function<String,String> featureToAttrClassMapper)
  {
    SzAttributeSearchResult result = (searchResult != null)
        ? searchResult : new SzAttributeSearchResult();

    Function<String,String> mapper = featureToAttrClassMapper;

    SzBaseRelatedEntity.parseBaseRelatedEntity(result, jsonObject, mapper);

    JsonObject entityObject = JsonUtils.getJsonObject(jsonObject, "ENTITY");
    if (entityObject == null) {
      entityObject = jsonObject;
    }
    JsonArray relatedArray = JsonUtils.getJsonArray(entityObject,
                                                    "RELATED_ENTITIES");

    List<SzRelatedEntity> relatedEntities = null;
    if (relatedArray != null) {
      relatedEntities = SzRelatedEntity.parseRelatedEntityList(null,
                                                               relatedArray,
                                                               mapper);
    }

    SzAttributeSearchResultType resultType = null;
    switch (result.getMatchLevel()) {
      case 1:
        resultType = MATCH;
        break;
      case 2:
        resultType = POSSIBLE_MATCH;
        break;
      case 3:
        resultType = POSSIBLE_RELATION;
        break;
      case 4:
        resultType = NAME_ONLY_MATCH;
        break;
    }
    result.setResultType(resultType);
    if (relatedEntities != null) {
      result.setRelatedEntities(relatedEntities);
    }

    // check if we have a MATCH_INFO object and if so use it for match information
    JsonObject matchInfo = JsonUtils.getJsonObject(jsonObject,"MATCH_INFO");

    // parse the feature scores
    if (matchInfo != null) {
      JsonObject featureScoresObject
          = JsonUtils.getJsonObject(matchInfo, "FEATURE_SCORES");

      Map<String, List<SzSearchFeatureScore>> featureScoreMap
          = new LinkedHashMap<>();

      featureScoresObject.entrySet().forEach(entry -> {
        String featureType = entry.getKey();

        JsonValue jsonValue = entry.getValue();

        JsonArray jsonArray = jsonValue.asJsonArray();

        List<SzSearchFeatureScore> featureScores
            = SzSearchFeatureScore.parseFeatureScoreList(jsonArray, featureType);

        featureScoreMap.put(featureType, featureScores);

        // check if this is for a name
        if (featureType.equals("NAME")) {
          // find the best name score
          Integer bestNameScore = null;

          // iterate through the search feature scores
          for (SzSearchFeatureScore featureScore : featureScores) {
            // get the name scoring details
            SzNameScoring nameScoring = featureScore.getNameScoringDetails();
            if (nameScoring == null) continue;

            // retrieve the full name and org name score
            Integer fullNameScore = nameScoring.getFullNameScore();
            Integer orgNameScore  = nameScoring.getOrgNameScore();

            // check for null values and get the maximum of the two scores
            if (fullNameScore == null) fullNameScore = -1;
            if (orgNameScore == null) orgNameScore = -1;
            int maxScore = Integer.max(fullNameScore, orgNameScore);

            // if we have a positive score and it is higher, then update
            if (maxScore > 0
                && (bestNameScore == null || maxScore > bestNameScore))
            {
              bestNameScore = maxScore;
            }
          }

          // set the best name score
          result.setBestNameScore(bestNameScore);
        }
      });
      // set the feature scores
      result.setFeatureScores(featureScoreMap);
    }

    // return the result
    return result;
  }

  @Override
  public String toString() {
    return "SzAttributeSearchResult{" +
        super.toString() +
        ", resultType=" + this.resultType +
        ", bestNameScore=" + this.bestNameScore +
        ", featureScores=" + this.featureScores +
        ", relatedEntities=" + this.relatedEntities +
        '}';
  }
}
