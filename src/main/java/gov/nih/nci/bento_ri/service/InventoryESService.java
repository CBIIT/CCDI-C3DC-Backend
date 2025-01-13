package gov.nih.nci.bento_ri.service;

import com.google.gson.*;

import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.service.RedisService;
import gov.nih.nci.bento.service.connector.AWSClient;
import gov.nih.nci.bento.service.connector.AbstractClient;
import gov.nih.nci.bento.service.connector.DefaultClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.*;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.springframework.beans.factory.annotation.Autowired;
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
    final Set<String> PARTICIPANT_PARAMS = Set.of("participant_pk", "race", "sex_at_birth");
    final Set<String> DIAGNOSIS_PARAMS = Set.of(
        "age_at_diagnosis", "anatomic_site", "diagnosis_basis",
        "diagnosis", "diagnosis_classification_system",
        "disease_phase"
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

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private static final Logger logger = LogManager.getLogger(RedisService.class);

    @Autowired
    private ConfigurationDAO config;

    private RestClient client;

    private Gson gson = new GsonBuilder().serializeNulls().create();

    private InventoryESService(ConfigurationDAO config) {
        super(config);
        this.gson = new GsonBuilder().serializeNulls().create();
        logger.info("Initializing Elasticsearch client");
        // Base on host name to use signed request (AWS) or not (local)
        AbstractClient abstractClient = config.isEsSignRequests() ? new AWSClient(config) : new DefaultClient(config);
        client = abstractClient.getLowLevelElasticClient();
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

        List<Object> filter = new ArrayList<>();
        List<Object> participant_filters = new ArrayList<>();
        List<Object> diagnosis_filters = new ArrayList<>();
        List<Object> survival_filters = new ArrayList<>();
        List<Object> treatment_filters = new ArrayList<>();
        List<Object> treatment_response_filters = new ArrayList<>();
        
        for (String key: params.keySet()) {
            String finalKey = key;
            if (excludedParams.contains(finalKey)) {
                continue;
            }

            if (rangeParams.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Integer> bounds = (List<Integer>) params.get(key);
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
                    if (!indexType.equals("diagnoses") && key.equals("age_at_diagnosis")) {
                        diagnosis_filters.add(Map.of(
                            "range", Map.of("diagnoses." + key, range)
                        ));
                    } else if (!indexType.equals("survivals") && key.equals("age_at_last_known_survival_status")) {
                        survival_filters.add(Map.of(
                            "range", Map.of("survivals." + key, range)
                        ));
                    } else if (TREATMENT_PARAMS.contains(key) && !indexType.equals("treatments")) {
                        treatment_filters.add(Map.of(
                            "range", Map.of("treatments." + key, range)
                        ));
                    } else if (TREATMENT_RESPONSE_PARAMS.contains(key) && !indexType.equals("treatment_responses")) {
                        treatment_response_filters.add(Map.of(
                            "range", Map.of("treatment_responses." + key, range)
                        ));
                    } else {
                        filter.add(Map.of(
                            "range", Map.of(key, range)
                        ));
                    }
                }
            } else {
                // Term parameters (default)
                List<String> valueSet = (List<String>) params.get(key);
                
                if (key.equals("participant_ids")) {
                    key = "participant_id";
                } else if (key.equals("participant_pks")) {
                    key = "participant_pk";
                }

                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    if (DIAGNOSIS_PARAMS.contains(key) && !indexType.equals("diagnoses")) {
                        diagnosis_filters.add(Map.of(
                            "terms", Map.of("diagnoses." + key, valueSet)
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
                    } else {
                        filter.add(Map.of(
                            "terms", Map.of(key, valueSet)
                        ));
                    }
                }
            }
        }

        int FilterLen = filter.size();
        int participantFilterLen = participant_filters.size();
        int diagnosisFilterLen = diagnosis_filters.size();
        int survivalFilterLen = survival_filters.size();
        int treatmentFilterLen = treatment_filters.size();
        int treatmentResponseFilterLen = treatment_response_filters.size();
        if (FilterLen + participantFilterLen + diagnosisFilterLen + survivalFilterLen + treatmentFilterLen + treatmentResponseFilterLen == 0) {
            result.put("query", Map.of("match_all", Map.of()));
        } else {
            if (participantFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "participant_filters", "query", Map.of("bool", Map.of("filter", participant_filters)), "inner_hits", Map.of())));
            }
            if (diagnosisFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "diagnoses", "query", Map.of("bool", Map.of("filter", diagnosis_filters)), "inner_hits", Map.of())));
            }
            if (survivalFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "survivals", "query", Map.of("bool", Map.of("filter", survival_filters)), "inner_hits", Map.of())));
            }
            if (treatmentFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "treatments", "query", Map.of("bool", Map.of("filter", treatment_filters)), "inner_hits", Map.of())));
            }
            if (treatmentResponseFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "treatment_responses", "query", Map.of("bool", Map.of("filter", treatment_response_filters)), "inner_hits", Map.of())));
            }
            result.put("query", Map.of("bool", Map.of("filter", filter)));
        }
        
        return result;
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

        newQuery.put("aggs", counts);
        newQuery.put("size", 0);
        return newQuery;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String cardinalityAggName, List<String> only_includes) {
        return addAggregations(query, termAggNames, cardinalityAggName, new String[]{}, only_includes);
    }

    public Map<String, Object> addNodeCountAggregations(Map<String, Object> query, String nodeName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);

        // "aggs" : {
        //     "langs" : {
        //         "terms" : { "field" : "language",  "size" : 500 }
        //     }
        // }

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(nodeName, Map.of("terms", Map.ofEntries(
            Map.entry("field", nodeName),
            Map.entry("size", 10000)
        )));
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addRangeCountAggregations(Map<String, Object> query, String rangeAggName, String cardinalityAggName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);

        Map<String, Object> fields = new HashMap<String, Object>();
        Map<String, Object> subField = new HashMap<String, Object>();
        Map<String, Object> subField_ranges = new HashMap<String, Object>();
        subField_ranges.put("field", rangeAggName);
        subField_ranges.put("ranges", Set.of(Map.of("key", "0 - 4", "from", 0, "to", 4 * 365), Map.of("key", "5 - 9", "from", 4 * 365, "to", 9 * 365), Map.of("key", "10 - 14", "from", 9 * 365, "to", 14 * 365), Map.of("key", "15 - 19", "from", 14 * 365, "to", 19 * 365), Map.of("key", "20 - 29", "from", 19 * 365, "to", 29 * 365), Map.of("key", "> 29", "from", 29 * 365)));
        
        subField.put("range", subField_ranges);
        if (! (cardinalityAggName == null)) {
            subField.put("aggs", Map.of("cardinality_count", Map.of("cardinality", Map.of("field", cardinalityAggName, "precision_threshold", 40000))));
        }
        fields.put(rangeAggName, subField);
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
        return Map.of("cardinality_count", Map.of("cardinality", Map.of("field", cardinalityAggName, "precision_threshold", 40000)));
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