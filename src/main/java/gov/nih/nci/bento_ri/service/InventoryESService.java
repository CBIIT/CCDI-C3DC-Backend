package gov.nih.nci.bento_ri.service;

import com.google.gson.*;

import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.utility.TypeChecker;

import org.opensearch.client.*;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.reflect.TypeToken;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service("InventoryESService")
public class InventoryESService extends ESService {
    public static final String SCROLL_ENDPOINT = "/_search/scroll";
    public static final String JSON_OBJECT = "jsonObject";
    public static final String AGGS = "aggs";
    public static final int MAX_ES_SIZE = 60000;
    public static final int SCROLL_THRESHOLD = 10000;
    final Set<String> PARTICIPANT_PARAMS = Set.of(
        "id", "participant_id", "race", "sex_at_birth"
    );
    final Set<String> DIAGNOSIS_PARAMS = Set.of(
        "age_at_diagnosis", "anatomic_site", "diagnosis_basis",
        "diagnosis", "diagnosis_classification_system",
        "disease_phase"
    );
    final Set<String> GENETIC_ANALYSIS_PARAMS = Set.of(
        "alteration", "alteration_type", "fusion_partner_gene",
        "gene_symbol", "reported_significance",
        "reported_significance_system", "status"
    );
    final Set<String> STUDY_PARAMS = Set.of(
        "dbgap_accession", "study_name"
    );
    final Set<String> SURVIVAL_PARAMS = Set.of(
        "age_at_last_known_survival_status", "cause_of_death",
        "first_event", "last_known_survival_status"
    );
    final Set<String> TREATMENT_PARAMS = Set.of(
        "age_at_treatment_start", "age_at_treatment_end",
        "treatment_type", "treatment_agent"
    );
    final Set<String> TREATMENT_RESPONSE_PARAMS = Set.of(
        "response", "age_at_response",
        "response_category", "response_system"
    );

    // Map of index type to respective params
    public final Map<String, Set<String>> INDEX_TO_PARAMS = Map.ofEntries(
        Map.entry("participants", PARTICIPANT_PARAMS),
        Map.entry("diagnoses", DIAGNOSIS_PARAMS),
        Map.entry("genetic_analyses", GENETIC_ANALYSIS_PARAMS),
        Map.entry("studies", STUDY_PARAMS),
        Map.entry("survivals", SURVIVAL_PARAMS),
        Map.entry("treatments", TREATMENT_PARAMS),
        Map.entry("treatment_responses", TREATMENT_RESPONSE_PARAMS)
    );

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private Gson gson = new GsonBuilder().serializeNulls().create();

    private InventoryESService(ConfigurationDAO config) {
        super(config);
        this.gson = new GsonBuilder().serializeNulls().create();
    }

    /**
     * Counts how many results there are for a facet filter value
     * @param params GraphQL variables
     * @param rangeParams GraphQL variables that are numeric
     * @param index The Opensearch index that the request is for
     * @param field The facet filter to recount
     * @param value The value of the facet filter to recount
     * @return
     * @throws IOException
     */
    public Integer recountFacetFilterValue(Map<String, Object> params, Set<String> rangeParams, String index, String field, String value) throws IOException {
        Map<String, Object> query_4_update = buildFacetFilterQuery(params, rangeParams, Set.of(field), "participants");
        Map<String, Integer> updated_values;
        Request request = new Request("GET", "/participants/_search");
        JsonObject jsonObject;
        String query_4_update_json;

        // Create reverse_nested aggregation
        query_4_update = addCustomAggregations(query_4_update, "facetAgg", field, index);
        query_4_update_json = gson.toJson(query_4_update);
        request.setJsonEntity(query_4_update_json);
        jsonObject = send(request);

        // Retrieve new counts
        updated_values = collectCustomTerms(jsonObject, "facetAgg");

        return updated_values.get(value);
    }

    /**
     * Builds the Opensearch request body for facet filtering
     * @param params GraphQL variables
     * @param rangeParams GraphQL variables that are numeric
     * @param excludedParams GraphQL variables to skip
     * @param indexType The Opensearch index that the request is for
     * @return
     * @throws IOException
     */
    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams, Set<String> excludedParams, String indexType) throws IOException {
        Map<String, Object> result = new HashMap<>();

        // Add unknownAges parameters to excluded parameters to prevent them from being processed as regular filters
        Set<String> localExcludedParams = new HashSet<>(excludedParams);
        for (String key : params.keySet()) {
            if (key.endsWith("_unknownAges")) {
                localExcludedParams.add(key);
            }
        }

        List<Object> filter = new ArrayList<>();
        // Map of nested filters
        Map<String, List<Object>> NESTED_FILTERS = Map.ofEntries(
            Map.entry("participants", new ArrayList<>()),
            Map.entry("diagnoses", new ArrayList<>()),
            Map.entry("genetic_analyses", new ArrayList<>()),
            Map.entry("survivals", new ArrayList<>()),
            Map.entry("treatments", new ArrayList<>()),
            Map.entry("treatment_responses", new ArrayList<>())
        );
        List<Object> participant_filters = new ArrayList<>();
        List<Object> diagnosis_filters = new ArrayList<>();
        List<Object> genetic_analysis_filters = new ArrayList<>();
        List<Object> survival_filters = new ArrayList<>();
        List<Object> treatment_filters = new ArrayList<>();
        List<Object> treatment_response_filters = new ArrayList<>();
        
        for (String key: params.keySet()) {
            String finalKey = key;
            if (localExcludedParams.contains(finalKey)) {
                continue;
            }

            // Determine the nested property for the key, if it's nested
            String nestedProperty = null;
            String formattedNestedProperty = null;
            for (String index : INDEX_TO_PARAMS.keySet()) {
                if (INDEX_TO_PARAMS.get(index).contains(key)) {
                    nestedProperty = index;
                    formattedNestedProperty = index;
                    break;
                }
            }

            // Special treatment for participants, which doesn't use a plural nested property name
            if (nestedProperty != null && nestedProperty.equals("participants")) {
                formattedNestedProperty = "participant";
            }

            if (rangeParams.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Integer> bounds = null;
                Object boundsRaw = params.get(key);

                if (TypeChecker.isOfType(boundsRaw, new TypeToken<List<Integer>>() {})) {
                    @SuppressWarnings("unchecked")
                    List<Integer> castedBounds = (List<Integer>) boundsRaw;
                    bounds = castedBounds;
                }

                // Check if unknownAges parameter exists to determine if we should include unknown values
                String unknownAgesKey = key + "_unknownAges";
                String unknownAgesValue = "include";
                boolean includeUnknown = true; // Default to including unknown values
                if (params.containsKey(unknownAgesKey)) {
                    @SuppressWarnings("unchecked") // Type is guaranteed by GraphQL schema
                    List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                    // Only consider unknownAges parameter if it has a meaningful value
                    if (!(unknownAgesValues == null || unknownAgesValues.isEmpty() || unknownAgesValues.get(0).equals(""))) {
                        includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                        unknownAgesValue = unknownAgesValues.get(0).toLowerCase();
                    }
                }

                if (bounds.size() >= 2) {
                    Integer lower = bounds.get(0);
                    Integer higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    Map<String, Integer> range = new HashMap<>();
                    if (lower != null) {
                        range.put("gte", lower);
                    }
                    if (higher != null) {
                        range.put("lte", higher);
                    }

                    if (INDEX_TO_PARAMS.get(indexType).contains(key)) { // Key is a top-level param for index
                        if (includeUnknown) {
                            // Include unknown values (-999)
                            filter.add(Map.of(
                                "bool", Map.of("should", List.of(
                                    Map.of("range", Map.of(key, range)),
                                    Map.of("term", Map.of(key, -999))
                                ))
                            ));
                        } else {
                            // Use normal range filter when unknownAges parameter is specified
                            filter.add(Map.of(
                                "range", Map.of(key, range)
                            ));
                        }
                    } else { // Key is a nested param for index
                        if (includeUnknown) {
                            // Include unknown values (-999)
                            NESTED_FILTERS.get(nestedProperty).add(Map.of(
                                "bool", Map.of("should", List.of(
                                    Map.of("range", Map.of(formattedNestedProperty + "." + key, range)),
                                    Map.of("term", Map.of(formattedNestedProperty + "." + key, -999))
                                ))
                            ));
                        } else {
                            // Use normal range filter when unknownAges parameter is specified
                            NESTED_FILTERS.get(nestedProperty).add(Map.of(
                                "range", Map.of(formattedNestedProperty + "." + key, range)
                            ));
                        }
                    }
                    // if (!indexType.equals("diagnoses") && DIAGNOSIS_PARAMS.contains(key)) {
                    //     if (includeUnknown) {
                    //         // Include unknown values (-999)
                    //         diagnosis_filters.add(Map.of(
                    //             "bool", Map.of("should", List.of(
                    //                 Map.of("range", Map.of("diagnoses." + key, range)),
                    //                 Map.of("term", Map.of("diagnoses." + key, -999))
                    //             ))
                    //         ));
                    //     } else {
                    //         // Use normal range filter when unknownAges parameter is specified
                    //         diagnosis_filters.add(Map.of(
                    //             "range", Map.of("diagnoses." + key, range)
                    //         ));
                    //     }
                    // } else if (!indexType.equals("survivals") && SURVIVAL_PARAMS.contains(key)) {
                    //     if (includeUnknown) {
                    //         // Include unknown values (-999)
                    //         survival_filters.add(Map.of(
                    //             "bool", Map.of("should", List.of(
                    //                 Map.of("range", Map.of("survivals." + key, range)),
                    //                 Map.of("term", Map.of("survivals." + key, -999))
                    //             ))
                    //         ));
                    //     } else {
                    //         // Use normal range filter when unknownAges parameter is specified
                    //         survival_filters.add(Map.of(
                    //             "range", Map.of("survivals." + key, range)
                    //         ));
                    //     }
                    // } else if (!indexType.equals("treatments") && TREATMENT_PARAMS.contains(key)) {
                    //     if (includeUnknown) {
                    //         // Include unknown values (-999)
                    //         treatment_filters.add(Map.of(
                    //             "bool", Map.of("should", List.of(
                    //                 Map.of("range", Map.of("treatments." + key, range)),
                    //                 Map.of("term", Map.of("treatments." + key, -999))
                    //             ))
                    //         ));
                    //     } else {
                    //         // Use normal range filter when unknownAges parameter is specified
                    //         treatment_filters.add(Map.of(
                    //             "range", Map.of("treatments." + key, range)
                    //         ));
                    //     }
                    // } else if (!indexType.equals("treatment_responses") && TREATMENT_RESPONSE_PARAMS.contains(key)) {
                    //     if (includeUnknown) {
                    //         // Include unknown values (-999)
                    //         treatment_response_filters.add(Map.of(
                    //             "bool", Map.of("should", List.of(
                    //                 Map.of("range", Map.of("treatment_responses." + key, range)),
                    //                 Map.of("term", Map.of("treatment_responses." + key, -999))
                    //             ))
                    //         ));
                    //     } else {
                    //         // Use normal range filter when unknownAges parameter is specified
                    //         treatment_response_filters.add(Map.of(
                    //             "range", Map.of("treatment_responses." + key, range)
                    //         ));
                    //     }
                    // } else {
                    //     filter.add(Map.of(
                    //         "range", Map.of(key, range)
                    //     ));
                    // }
                }

                if (INDEX_TO_PARAMS.get(indexType).contains(key)) { // Key is a top-level param for index
                    if (unknownAgesValue.equals("exclude")) {
                        filter.add(Map.of(
                            "bool", Map.of("must_not", Map.of("terms", Map.of(key, List.of(-999))))
                        ));
                    } else if (unknownAgesValue.equals("only")) {
                        filter.add(Map.of(
                            "terms", Map.of(key, List.of(-999))
                        ));
                    }
                } else { // Key is a nested param for index
                    if (unknownAgesValue.equals("exclude")) {
                        NESTED_FILTERS.get(nestedProperty).add(Map.of(
                            "bool", Map.of("must_not", Map.of("terms", Map.of(formattedNestedProperty + "." + key, List.of(-999))))
                        ));
                    } else if (unknownAgesValue.equals("only")) {
                        NESTED_FILTERS.get(nestedProperty).add(Map.of(
                            "terms", Map.of(formattedNestedProperty + "." + key, List.of(-999))
                        ));
                    }
                }
            } else {
                // Term parameters (default)
                List<String> valueSet = null;
                Object valueSetRaw = params.get(key);

                if (TypeChecker.isOfType(valueSetRaw, new TypeToken<List<String>>() {})) {
                    @SuppressWarnings("unchecked")
                    List<String> castedValueSet = (List<String>) valueSetRaw;
                    valueSet = castedValueSet;
                }
                
                if (key.equals("participant_pk") && indexType.equals("participants")) {
                    key = "id";
                }

                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    if (DIAGNOSIS_PARAMS.contains(key) && !indexType.equals("diagnoses")) {
                        diagnosis_filters.add(Map.of(
                            "terms", Map.of("diagnoses." + key, valueSet)
                        ));
                    } else if (GENETIC_ANALYSIS_PARAMS.contains(key) && !indexType.equals("genetic_analyses")) {
                        genetic_analysis_filters.add(Map.of(
                            "terms", Map.of("genetic_analyses." + key, valueSet)
                        ));
                    } else if (SURVIVAL_PARAMS.contains(key) && !indexType.equals("survivals")) {
                        survival_filters.add(Map.of(
                            "terms", Map.of("survivals." + key, valueSet)
                        ));
                    } else if (TREATMENT_PARAMS.contains(key) && !indexType.equals("treatments")) {
                        treatment_filters.add(Map.of(
                            "terms", Map.of("treatments." + key, valueSet)
                        ));
                    } else if (TREATMENT_RESPONSE_PARAMS.contains(key) && !indexType.equals("treatment_responses")) {
                        treatment_response_filters.add(Map.of(
                            "terms", Map.of("treatment_responses." + key, valueSet)
                        ));
                    } else if (PARTICIPANT_PARAMS.contains(key) && !indexType.equals("participants")) {// Filter by nested Participant property
                        participant_filters.add(Map.of(
                            "terms", Map.of("participant." + key, valueSet)
                        ));
                    } else {
                        filter.add(Map.of(
                            "terms", Map.of(key, valueSet)
                        ));
                    }
                }
            }
        }

        int participantFilterLen = NESTED_FILTERS.get("participants").size();
        int diagnosisFilterLen = NESTED_FILTERS.get("diagnoses").size();
        int geneticAnalysisFilterLen = NESTED_FILTERS.get("genetic_analyses").size();
        int survivalFilterLen = NESTED_FILTERS.get("survivals").size();
        int treatmentFilterLen = NESTED_FILTERS.get("treatments").size();
        int treatmentResponseFilterLen = NESTED_FILTERS.get("treatment_responses").size();

        boolean hasFilters = filter.size() > 0 || NESTED_FILTERS.values().stream().anyMatch(list -> !list.isEmpty());

        if (!hasFilters) {
            result.put("query", Map.of("match_all", Map.of()));
        } else {
            for (String nestedProperty : NESTED_FILTERS.keySet()) {
                List<Object> filters = NESTED_FILTERS.get(nestedProperty);
                String formattedNestedProperty = nestedProperty;

                // Special treatment for participants, which doesn't use a plural nested property name
                if (nestedProperty.equals("participants")) {
                    formattedNestedProperty = "participant";
                }

                if (filters.size() > 0) {
                    filter.add(Map.of("nested", Map.of("path", formattedNestedProperty, "query", Map.of("bool", Map.of("filter", filters)), "inner_hits", Map.of())));
                }
            }
            // if (participantFilterLen > 0) {
            //     filter.add(Map.of("nested", Map.of("path", "participant", "query", Map.of("bool", Map.of("filter", participant_filters)), "inner_hits", Map.of())));
            // }
            // if (diagnosisFilterLen > 0) {
            //     filter.add(Map.of("nested", Map.of("path", "diagnoses", "query", Map.of("bool", Map.of("filter", diagnosis_filters)), "inner_hits", Map.of())));
            // }
            // if (geneticAnalysisFilterLen > 0) {
            //     filter.add(Map.of("nested", Map.of("path", "genetic_analyses", "query", Map.of("bool", Map.of("filter", genetic_analysis_filters)), "inner_hits", Map.of())));
            // }
            // if (survivalFilterLen > 0) {
            //     filter.add(Map.of("nested", Map.of("path", "survivals", "query", Map.of("bool", Map.of("filter", survival_filters)), "inner_hits", Map.of())));
            // }
            // if (treatmentFilterLen > 0) {
            //     filter.add(Map.of("nested", Map.of("path", "treatments", "query", Map.of("bool", Map.of("filter", treatment_filters)), "inner_hits", Map.of())));
            // }
            // if (treatmentResponseFilterLen > 0) {
            //     filter.add(Map.of("nested", Map.of("path", "treatment_responses", "query", Map.of("bool", Map.of("filter", treatment_response_filters)), "inner_hits", Map.of())));
            // }
            result.put("query", Map.of("bool", Map.of("filter", filter)));
        }
        
        return result;
    }

    public List<String> getBucketNames(String property, Map<String, Object> params, Set<String> rangeParams, String cardinalityAggName, String index, String endpoint) throws IOException {
        List<String> bucketNames = new ArrayList<String>();
        Map<String, Object> query = buildFacetFilterQuery(params, rangeParams, Set.of(), index);

        // TODO: buckets for numeric ranges, when such a feature is needed
        // stub

        // Add aggs clause to Opensearch query
        String[] aggNames = new String[] {property};
        query = addAggregations(query, aggNames, cardinalityAggName, List.of());

        // Send Opensearch request and retrieve list of buckets
        Request request = new Request("GET", endpoint);
        String jsonizedRequest = gson.toJson(query);
        request.setJsonEntity(jsonizedRequest);
        JsonObject jsonObject = send(request);
        Map<String, JsonArray> aggs = collectTermAggs(jsonObject, aggNames);
        JsonArray buckets = aggs.get(property);

        if (buckets != null) {
            for (JsonElement bucket : buckets) {
                JsonObject bucketObj = bucket.getAsJsonObject();
                if (bucketObj.has("key")) {
                    bucketNames.add(bucketObj.get("key").getAsString());
                }
            }
        }

        return bucketNames;
    }

    /**
     * Queries the /_count Opensearch endpoint and returns the number of hits
     * @param query Opensearch query
     * @param index Name of the index to query
     * @return
     * @throws IOException
     */
    public int getCount(Map<String, Object> query, String index) throws IOException {
        Request request = new Request("GET", String.format("/%s/_count", index));
        String queryJson = gson.toJson(query);
        JsonObject recountResult;
        int newCount;

        request.setJsonEntity(queryJson);
        recountResult = send(request);
        newCount = recountResult.get("count").getAsInt();

        return newCount;
    }

    /**
     * Count unique values of the given fields
     * @param query The base Opensearch query map to modify
     * @param termAggNames The fields whose values to count
     * @return A modified Opensearch query map
     */
    public Map<String, Object> countValues(Map<String, Object> query, String[] termAggNames) {
        Map<String, Object> newQuery = new HashMap<>(query);
        Map<String, Object> counts = new HashMap<String, Object>();

        // Add counts for each field
        for (String term : termAggNames) {
            counts.put("num_values_of_" + term, Map.ofEntries(
                Map.entry("cardinality", Map.ofEntries(
                    Map.entry("field", term)
                ))
            ));
        }

        newQuery.put("size", 0);
        newQuery.put("aggs", counts);

        return newQuery;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String cardinalityAggName, List<String> only_includes) {
        return addAggregations(query, termAggNames, cardinalityAggName, new String[]{}, only_includes);
    }

    public Map<String, Object> addNodeCountAggregations(Map<String, Object> query, String nodeName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        Map<String, Object> fields = new HashMap<String, Object>();

        fields.put(nodeName, Map.of("terms", Map.ofEntries(
            Map.entry("field", nodeName),
            Map.entry("size", 10000)
        )));
        newQuery.put("size", 0);
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addRangeCountAggregations(Map<String, Object> query, String rangeAggName, String cardinalityAggName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        Map<String, Object> fields = new HashMap<String, Object>();
        Map<String, Object> subField = new HashMap<String, Object>();
        Map<String, Object> subField_ranges = new HashMap<String, Object>();

        subField_ranges.put("field", rangeAggName);

        // Opensearch ranges are [from, to)
        subField_ranges.put("ranges", Set.of(
            Map.of(
                "key", "0 - 4",
                "from", 0,
                "to", 5 * 365
            ), Map.of(
                "key", "5 - 9",
                "from", 5 * 365,
                "to", 10 * 365
            ), Map.of(
                "key", "10 - 14",
                "from", 10 * 365,
                "to", 15 * 365
            ), Map.of(
                "key", "15 - 19",
                "from", 15 * 365,
                "to", 20 * 365
            ), Map.of(
                "key", "20 - 29",
                "from", 20 * 365,
                "to", 30 * 365
            ), Map.of(
                "key", "> 29",
                "from", 30 * 365
            )
        ));
        subField.put("range", subField_ranges);

        if (cardinalityAggName != null) {
            subField.put("aggs", addCardinalityHelper(cardinalityAggName));
        }

        fields.put(rangeAggName, subField);
        newQuery.put("size", 0);
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addRangeAggregations(Map<String, Object> query, String rangeAggName, List<String> only_includes) {
        Map<String, Object> newQuery = new HashMap<>(query);
        Map<String, Object> fields = new HashMap<String, Object>();
        Map<String, Object> subField = new HashMap<String, Object>();

        subField.put("filter", Map.of("range", Map.of(rangeAggName, Map.of("gt", -1))));
        subField.put("aggs", Map.of("range_stats", Map.of("stats", Map.of("field", rangeAggName))));
        fields.put("inner", subField);
        newQuery.put("size", 0);
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String subCardinalityAggName, String[] rangeAggNames, List<String> only_includes) {
        Map<String, Object> newQuery = new HashMap<>(query);
        Map<String, Object> fields = new HashMap<String, Object>();

        for (String field: termAggNames) {
            Map<String, Object> subField = new HashMap<String, Object>();
            subField.put("field", field);
            subField.put("size", 100000);
            if (only_includes.size() > 0) {
                subField.put("include", only_includes);
            }
            if (! (subCardinalityAggName == null)) {
                fields.put(field, Map.of("terms", subField, "aggs", addCardinalityHelper(subCardinalityAggName)));
            } else {
                fields.put(field, Map.of("terms", subField));
            }
        }

        newQuery.put("size", 0);
        newQuery.put("aggs", fields);

        return newQuery;
    }

    // Builds a reverse_nested Opensearch query for redoing facet filter counts
    public Map<String, Object> addCustomAggregations(Map<String, Object> query, String aggName, String field, String nestedProperty) {
        Map<String, Object> newQuery = new HashMap<>(query);
        Map<String, Object> aggSection = new HashMap<String, Object>();
        Map<String, Object> aggSubSection = new HashMap<String, Object>();

        aggSubSection.put("agg_buckets", Map.of("terms", Map.of("field", nestedProperty + "." + field, "size", 1000), "aggs", Map.of("top_reverse_nested", Map.of("reverse_nested", Map.of()))));
        aggSection.put(aggName, Map.of("nested", Map.of("path", nestedProperty), "aggs", aggSubSection));
        newQuery.put("size", 0);
        newQuery.put("aggs", aggSection);

        return newQuery;
    }

    public Map<String, Object> addCardinalityHelper(String cardinalityAggName) {
        int dotIndex = cardinalityAggName.indexOf("."); // Look for period (.) in cardinal property's name
        boolean isNested = (dotIndex != -1); // Determine whether the cardinal property is nested
        Map<String, Object> cardinalityInnerClause = Map.ofEntries(
            Map.entry("cardinality", Map.ofEntries(
                Map.entry("field", cardinalityAggName),
                Map.entry("precision_threshold", 40000)
            ))
        );
        Map<String, Object> cardinalityClause = null;

        // Handle nesting
        if (isNested) {
            cardinalityClause = Map.ofEntries(
                Map.entry("cardinality_count", Map.ofEntries(
                    Map.entry("nested", Map.ofEntries(
                        Map.entry("path", cardinalityAggName.substring(0, dotIndex))
                    )),
                    Map.entry("aggs", Map.ofEntries(
                        Map.entry("nested_cardinality_count", cardinalityInnerClause)
                    ))
                ))
            );
        } else {
            cardinalityClause = Map.ofEntries(
                Map.entry("cardinality_count", cardinalityInnerClause)
            );
        }

        return cardinalityClause;
    }

    public Map<String, JsonArray> collectNodeCountAggs(JsonObject jsonObject, String nodeName) {
        Map<String, JsonArray> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");

        data.put(nodeName, aggs.getAsJsonObject(nodeName).getAsJsonArray("buckets"));

        return data;
    }

    public Map<String, JsonArray> collectRangCountAggs(JsonObject jsonObject, String rangeAggName) {
        Map<String, JsonArray> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");

        data.put(rangeAggName, aggs.getAsJsonObject(rangeAggName).getAsJsonArray("buckets"));

        return data;
    }

    public Map<String, JsonObject> collectRangAggs(JsonObject jsonObject, String rangeAggName) {
        Map<String, JsonObject> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");

        data.put(rangeAggName, aggs.getAsJsonObject("inner").getAsJsonObject("range_stats"));

        return data;
    }

    // Retrieves recalculated facet filter counts
    public Map<String, Integer> collectCustomTerms(JsonObject jsonObject, String aggName) {
        Map<String, Integer> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations").getAsJsonObject(aggName);
        JsonArray buckets = aggs.getAsJsonObject("agg_buckets").getAsJsonArray("buckets");

        for (var bucket: buckets) {
            data.put(bucket.getAsJsonObject().get("key").getAsString(), bucket.getAsJsonObject().getAsJsonObject("top_reverse_nested").get("doc_count").getAsInt());
        }

        return data;
    }
}