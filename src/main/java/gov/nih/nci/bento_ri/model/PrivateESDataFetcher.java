package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.utility.TypeChecker;
import gov.nih.nci.bento_ri.service.InventoryESService;
import graphql.schema.idl.RuntimeWiring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
public class PrivateESDataFetcher extends AbstractPrivateESDataFetcher {
    private static final Logger logger = LogManager.getLogger(PrivateESDataFetcher.class);
    private final YamlQueryFactory yamlQueryFactory;
    private InventoryESService inventoryESService;
    @Autowired
    private Cache<String, Object> caffeineCache;

    private Map<String, Map<String, Map<String, Integer>>> facetFilterThresholds;
    private Map<String, List<Map<String, String>>> facetFilters;

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    // Maximum numbers of buckets to show in cohort analyzer charts
    final int COHORT_CHART_BUCKET_LIMIT_HIGH = 20;
    final int COHORT_CHART_BUCKET_LIMIT_LOW = 5;

    final String STUDIES_FACET_END_POINT = "/study_participants/_search";
    final String COHORTS_END_POINT = "/cohorts/_search";
    final String GENETIC_ANALYSES_END_POINT = "/genetic_analyses/_search";
    final String PARTICIPANTS_END_POINT = "/participants/_search";
    final String SURVIVALS_END_POINT = "/survivals/_search";
    final String SYNONYMS_END_POINT = "/synonyms/_search";
    final String TREATMENTS_END_POINT = "/treatments/_search";
    final String TREATMENT_RESPONSES_END_POINT = "/treatment_responses/_search";
    final String DIAGNOSES_END_POINT = "/diagnoses/_search";
    final String HOME_STATS_END_POINT = "/home_stats/_search";
    final String STUDIES_END_POINT = "/studies/_search";
    final String SAMPLES_END_POINT = "/samples/_search";
    final Map<String, String> ENDPOINTS = Map.ofEntries(
        Map.entry("diagnoses", DIAGNOSES_END_POINT),
        Map.entry("genetic_analyses", GENETIC_ANALYSES_END_POINT),
        Map.entry("participants", PARTICIPANTS_END_POINT),
        Map.entry("studies", STUDIES_END_POINT),
        Map.entry("survivals", SURVIVALS_END_POINT),
        Map.entry("synonyms", SURVIVALS_END_POINT),
        Map.entry("treatments", TREATMENTS_END_POINT),
        Map.entry("treatment_responses", TREATMENT_RESPONSES_END_POINT)
    );

    // For slider fields
    final Set<String> RANGE_PARAMS = Set.of(
        // Diagnoses
        "age_at_diagnosis",
        // Survivals
        "age_at_last_known_survival_status",
        // Treatments
        "age_at_treatment_end", "age_at_treatment_start",
        // Treatment Responses
        "age_at_response"
    );

    final Set<String> BOOLEAN_PARAMS = Set.of("assay_method");

    final Set<String> ARRAY_PARAMS = Set.of("file_type");

    // For multiple selection from a list
    final Set<String> INCLUDE_PARAMS  = Set.of(
        // Demographics
        "race",
        // Diagnoses
        "anatomic_site", "diagnosis"
    );

    public PrivateESDataFetcher(InventoryESService esService) throws IOException {
        super(esService);
        inventoryESService = esService;
        yamlQueryFactory = new YamlQueryFactory(esService);

        // Load facet filters
        try {
            String facetFiltersPath = Const.YAML_QUERY.SUB_FOLDER + "facet_filters.yaml";
            ClassPathResource facetFiltersResource = new ClassPathResource(facetFiltersPath);
            InputStream facetFilterFileStream = facetFiltersResource.getInputStream();
            Yaml facetFilterYaml = new Yaml();
            this.facetFilters = facetFilterYaml.load(facetFilterFileStream);
        } catch (IOException e) {
            logger.error("Error reading facet filters: "+ e.toString());
            throw new IOException(e.toString());
        }

        // Load facet filter recount thresholds
        try {
            String facetFilterThresholdsPath = Const.YAML_QUERY.SUB_FOLDER + "facet_filter_thresholds.yaml";
            ClassPathResource facetFilterThresholdsResource = new ClassPathResource(facetFilterThresholdsPath);
            InputStream facetFilterThresholdFileStream = facetFilterThresholdsResource.getInputStream();
            Yaml facetFilterThresholdYaml = new Yaml();
            this.facetFilterThresholds = facetFilterThresholdYaml.load(facetFilterThresholdFileStream);
        } catch (IOException e) {
            logger.error("Error reading facet filter recount thresholds: " + e.toString());
            throw new IOException(e.toString());
        }
    }

    @Override
    public RuntimeWiring buildRuntimeWiring() throws IOException {
        return RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("QueryType")
                        .dataFetchers(yamlQueryFactory.createYamlQueries(Const.ES_ACCESS_TYPE.PRIVATE))
                        .dataFetcher("idsLists", env -> idsLists())
                        .dataFetcher("getParticipants", env -> {
                            Map<String, Object> args = env.getArguments();
                            return getParticipants(args);
                        })
                        .dataFetcher("cohortCharts", env -> {
                            Map<String, Object> args = env.getArguments();
                            return cohortCharts(args);
                        })
                        .dataFetcher("cohortMetadata", env -> {
                            Map<String, Object> args = env.getArguments();
                            return cohortMetadata(args);
                        })
                        .dataFetcher("participantOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return participantOverview(args);
                        })
                        .dataFetcher("diagnosisOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return diagnosisOverview(args);
                        })
                        .dataFetcher("geneticAnalysisOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return geneticAnalysisOverview(args);
                        })
                        .dataFetcher("studyOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return studyOverview(args);
                        })
                        .dataFetcher("survivalOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return survivalOverview(args);
                        })
                        .dataFetcher("treatmentOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return treatmentOverview(args);
                        })
                        .dataFetcher("treatmentResponseOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return treatmentResponseOverview(args);
                        })
                        .dataFetcher("studyDetails", env -> {
                            Map<String, Object> args = env.getArguments();
                            return studyDetails(args);
                        })
                        .dataFetcher("studiesListing", env -> studiesListing())
                        .dataFetcher("numberOfDiseases", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfDiseases(args);
                        })
                        .dataFetcher("numberOfParticipants", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfParticipants(args);
                        })
                        .dataFetcher("numberOfReferenceFiles", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfReferenceFiles(args);
                        })
                        .dataFetcher("numberOfStudies", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfStudies(args);
                        })
                        .dataFetcher("numberOfSurvivals", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfSurvivals(args);
                        })
                        .dataFetcher("findParticipantIdsInList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return findParticipantIdsInList(args);
                        })
                )
                .build();
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return subjectCountBy(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), indexType);
        List<String> only_includes;
        List<String> valueSet = null;
        Object valueSetRaw = params.get(category);

        if (TypeChecker.isOfType(valueSetRaw, new TypeToken<List<String>>() {})) {
            @SuppressWarnings("unchecked")
            List<String> castedValueSet = (List<String>) params.get(category);
            valueSet = INCLUDE_PARAMS.contains(category) ? castedValueSet : List.of();
        }

        if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))){
            only_includes = valueSet;
        } else {
            only_includes = List.of();
        }
        return getGroupCount(category, query, endpoint, cardinalityAggName, only_includes);
    }

    private List<Map<String, Object>> subjectCountByRange(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return subjectCountByRange(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> subjectCountByRange(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), indexType);
        return getGroupCountByRange(category, query, endpoint, cardinalityAggName);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return filterSubjectCountBy(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, category), indexType);
        return getGroupCount(category, query, endpoint, cardinalityAggName, List.of());
    }

    private JsonArray getNodeCount(String category, Map<String, Object> query, String endpoint) throws IOException {
        query = inventoryESService.addNodeCountAggregations(query, category);
        String queryJson = gson.toJson(query);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(queryJson);
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectNodeCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return buckets;
    }

    private List<Map<String, Object>> getGroupCountByRange(String category, Map<String, Object> query, String endpoint, String cardinalityAggName) throws IOException {
        query = inventoryESService.addRangeCountAggregations(query, category, cardinalityAggName);
        String queryJson = gson.toJson(query);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(queryJson);
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectRangCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return getGroupCountHelper(buckets, cardinalityAggName);
    }

    private List<Map<String, Object>> getGroupCount(String category, Map<String, Object> query, String endpoint, String cardinalityAggName, List<String> only_includes) throws IOException {
        if (RANGE_PARAMS.contains(category)) { // Not sure if this case ever occurs, because there's a separate method getGroupCountByRange() for range params
            query = inventoryESService.addRangeAggregations(query, category, only_includes);
            Request request = new Request("GET", endpoint);
            String jsonizedRequest = gson.toJson(query);
            request.setJsonEntity(jsonizedRequest);
            JsonObject jsonObject = inventoryESService.send(request);
            Map<String, JsonObject> aggs = inventoryESService.collectRangAggs(jsonObject, category);
            JsonObject ranges = aggs.get(category);

            return getRangeGroupCountHelper(ranges);
        } else {
            String[] AGG_NAMES = new String[] {category};
            query = inventoryESService.addAggregations(query, AGG_NAMES, cardinalityAggName, only_includes);
            Request request = new Request("GET", endpoint);
            String jsonizedRequest = gson.toJson(query);
            request.setJsonEntity(jsonizedRequest);
            JsonObject jsonObject = inventoryESService.send(request);
            Map<String, JsonArray> aggs = inventoryESService.collectTermAggs(jsonObject, AGG_NAMES);
            JsonArray buckets = aggs.get(category);

            return getGroupCountHelper(buckets, cardinalityAggName);
        }
    }

    private List<Map<String, Object>> getRangeGroupCountHelper(JsonObject ranges) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        if (ranges.get("count").getAsInt() == 0) {
            data.add(Map.of("lowerBound", 0,
                    "subjects", 0,
                    "upperBound", 0
            ));
        } else {
            data.add(Map.of("lowerBound", ranges.get("min").getAsInt(),
                    "subjects", ranges.get("count").getAsInt(),
                    "upperBound", ranges.get("max").getAsInt()
            ));
        }
        return data;
    }

    private List<Map<String, Object>> getGroupCountHelper(JsonArray buckets, String cardinalityAggName) throws IOException {
        int dotIndex = cardinalityAggName == null ? -1 : cardinalityAggName.indexOf("."); // Look for period (.) in cardinal property's name
        boolean isNested = (dotIndex != -1); // Determine whether the cardinal property is nested
        List<Map<String, Object>> data = new ArrayList<>();

        for (JsonElement group: buckets) {
            int count = -1;

            if (cardinalityAggName == null) {
                count = group.getAsJsonObject().get("doc_count").getAsInt();
            } else if (isNested) {
                count = group.getAsJsonObject().get("cardinality_count").getAsJsonObject()
                        .get("nested_cardinality_count").getAsJsonObject().get("value").getAsInt();
            } else {
                count = group.getAsJsonObject().get("cardinality_count").getAsJsonObject().get("value").getAsInt();
            }

            data.add(Map.ofEntries(
                Map.entry("group", group.getAsJsonObject().get("key").getAsString()),
                Map.entry("subjects", count)
            ));
        }

        return data;
    }

    private Map<String, List<Object>> idsLists() throws IOException {
        String cacheKey = "participantIDs";
        Map<String, List<Object>> results = new HashMap<>();
        Map<String, List<Object>> data = null;
        Object dataRaw = caffeineCache.asMap().get(cacheKey);

        if (TypeChecker.isOfType(dataRaw, new TypeToken<Map<String, List<Object>>>() {})) {
            @SuppressWarnings("unchecked")
            Map<String, List<Object>> castedData = (Map<String, List<Object>>) dataRaw;
            data = castedData;
        }

        // Early return if cached
        if (data != null) {
            logger.info("hit cache!");
            return data;
        }

        Map<String, Object> participantParams = Map.ofEntries(
            Map.entry(OFFSET, 0),
            Map.entry(ORDER_BY, "participant_id"),
            Map.entry(PAGE_SIZE, ESService.MAX_ES_SIZE),
            Map.entry(SORT_DIRECTION, "asc")
        );
        Map<String, Object> synonymParams = Map.ofEntries(
            Map.entry(OFFSET, 0),
            Map.entry(ORDER_BY, "associated_id"),
            Map.entry(PAGE_SIZE, ESService.MAX_ES_SIZE),
            Map.entry(SORT_DIRECTION, "asc")
        );
        final List<Map<String, Object>> participantProperties = List.of(
            Map.ofEntries(
                Map.entry("gqlName", "participant_id"),
                Map.entry("osName", "participant_id")
            )
        );
        final List<Map<String, Object>> synonymProperties = List.of(
            Map.ofEntries(
                Map.entry("gqlName", "associated_id"),
                Map.entry("osName", "associated_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "participant_id"),
                Map.entry("osName", "participant_id")
            )
        );

        Map<String, Map<String, Object>> participantMapping = Map.ofEntries(// field -> sort field
            Map.entry("participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", false)
            ))
        );
        Map<String, Map<String, Object>> synonymMapping = Map.ofEntries(// field -> sort field
            Map.entry("associated_id", Map.ofEntries(
                Map.entry("osName", "associated_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", false)
            ))
        );

        List<Map<String, Object>> participantResults = overview(PARTICIPANTS_END_POINT, participantParams, participantProperties, "participant_id", participantMapping, "participants");
        List<Map<String, Object>> synonymResults = overview(SYNONYMS_END_POINT, synonymParams, synonymProperties, "associated_id", synonymMapping, "synonyms");

        results.put(
            "participantIds",
            participantResults.stream().map(participant -> participant.get("participant_id")).collect(Collectors.toList())
        );
        results.put(
            "associatedIds",
            synonymResults.stream().map(synonym -> Map.ofEntries(
                Map.entry("associated_id", synonym.get("associated_id")),
                Map.entry("participant_id", synonym.get("participant_id"))
            )).collect(Collectors.toList())
        );

        caffeineCache.put(cacheKey, results);

        return results;
    }

    /**
     * Returns facet filter counts and widget counts
     * Recalculates counts that might be inaccurate
     * @param params GraphQL variables
     * @return
     * @throws IOException
     */
    private Map<String, Object> getParticipants(Map<String, Object> params) throws IOException {
        String cacheKey = generateCacheKey(params);
        Map<String, Object> data = null;
        Object dataRaw = caffeineCache.asMap().get(cacheKey);

        if (TypeChecker.isOfType(dataRaw, new TypeToken<Map<String, Object>>() {})) {
            @SuppressWarnings("unchecked")
            Map<String, Object> castedData = (Map<String, Object>) dataRaw;
            data = castedData;
        }

        if (data != null) {
            logger.info("hit cache!");
            return data;
        }

        // logger.info("cache miss... querying for data.");
        data = new HashMap<>();

        final String CARDINALITY_AGG_NAME = "cardinality_agg_name";
        final String AGG_NAME = "agg_name";
        final String WIDGET_QUERY = "widget_count_name";
        final String FILTER_COUNT_QUERY = "filter_count_name";

        // Get disease counts for Explore page stats bar
        Map<String, Object> diseaseQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "diagnoses");
        String[] diseaseField = new String[]{"diagnosis"};
        diseaseQuery = inventoryESService.countValues(diseaseQuery, diseaseField);
        Request diseaseCountRequest = new Request("GET", DIAGNOSES_END_POINT);
        String diseaseQueryJson = gson.toJson(diseaseQuery);
        diseaseCountRequest.setJsonEntity(diseaseQueryJson);
        JsonObject diseaseCountResult = inventoryESService.send(diseaseCountRequest);
        int numberOfDiseases = diseaseCountResult.getAsJsonObject("aggregations")
            .getAsJsonObject("num_values_of_diagnosis").get("value").getAsInt();

        // Get Diagnosis counts for Explore page stats bar
        Map<String, Object> diagnosesQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "diagnoses");
        int numberOfDiagnoses = inventoryESService.getCount(diagnosesQuery, "diagnoses");

        // Get Diagnosis counts for Explore page stats bar
        Map<String, Object> geneticAnalysesQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "genetic_analyses");
        int numberOfGeneticAnalyses = inventoryESService.getCount(geneticAnalysesQuery, "genetic_analyses");

        // Get Survival counts for Explore page stats bar
        Map<String, Object> survivalsQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "survivals");
        int numberOfSurvivals = inventoryESService.getCount(survivalsQuery, "survivals");

        // Get Treatment counts for Explore page stats bar
        Map<String, Object> treatmentsQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "treatments");
        int numberOfTreatments = inventoryESService.getCount(treatmentsQuery, "treatments");

        // Get Treatment Response counts for Explore page stats bar
        Map<String, Object> treatmentResponsesQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "treatment_responses");
        int numberOfTreatmentResponses = inventoryESService.getCount(treatmentResponsesQuery, "treatment_responses");

        Map<String, Object> query_participants = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "participants");
        int numberOfStudies = getNodeCount("study_id", query_participants, PARTICIPANTS_END_POINT).size();
        
        Map<String, Object> participantsQuery = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), "participants");
        int numberOfParticipants = inventoryESService.getCount(participantsQuery, "participants");

        data.put("numberOfStudies", numberOfStudies);
        data.put("numberOfDiagnoses", numberOfDiagnoses);
        data.put("numberOfDiseases", numberOfDiseases);
        data.put("numberOfGeneticAnalyses", numberOfGeneticAnalyses);
        data.put("numberOfParticipants", numberOfParticipants);
        data.put("numberOfSurvivals", numberOfSurvivals);
        data.put("numberOfTreatments", numberOfTreatments);
        data.put("numberOfTreatmentResponses", numberOfTreatmentResponses);

        // Iterate through facet filters to query their counts
        for (Map.Entry<String, List<Map<String, String>>> entry : facetFilters.entrySet()) {
            String index = entry.getKey();
            List<Map<String, String>> filters = entry.getValue();
            String endpoint = ENDPOINTS.get(index);

            // Query this index for counts of each relevant facet filter
            for (Map<String, String> filter : filters) {
                String cardinalityAggName = filter.get(CARDINALITY_AGG_NAME);
                String field = filter.get(AGG_NAME);
                String filterCountQueryName = filter.get(FILTER_COUNT_QUERY);
                List<String> values = null;
                Object valuesRaw = params.get(field);
                String widgetQueryName = filter.get(WIDGET_QUERY);
                boolean shouldCheckThreshold = facetFilterThresholds.get(index).containsKey(field);
                List<Map<String, Object>> filterCounts = filterSubjectCountBy(field, params, endpoint, cardinalityAggName, index);
                Map<String, Integer> thresholds;
                List<Map<String, Object>> newFilterCounts;

                if (RANGE_PARAMS.contains(field)) {
                    data.put(filterCountQueryName, filterCounts.get(0));
                } else {
                    data.put(filterCountQueryName, filterCounts);
                }

                if (TypeChecker.isOfType(valuesRaw, new TypeToken<List<String>>() {})) {
                    @SuppressWarnings("unchecked")
                    List<String> castedValues = (List<String>) valuesRaw;
                    values = castedValues;
                }

                // Get widget counts
                if (widgetQueryName != null) {
                    // Fetch data for widgets
                    if (RANGE_PARAMS.contains(field)) {
                        List<Map<String, Object>> subjectCount = subjectCountByRange(field, params, endpoint, cardinalityAggName, index);
                        data.put(widgetQueryName, subjectCount);
                    } else if (params.containsKey(field) && values.size() > 0) {
                        List<Map<String, Object>> subjectCount = subjectCountBy(field, params, endpoint, cardinalityAggName, index);
                        data.put(widgetQueryName, subjectCount);
                    } else {
                        data.put(widgetQueryName, filterCounts);
                    }
                }

                // Nothing left to do if counts don't need to be redone
                if (!shouldCheckThreshold) {
                    continue;
                }

                thresholds = facetFilterThresholds.get(index).get(field);
                newFilterCounts = new ArrayList<Map<String, Object>>();

                // Do we have to replace the entire list?
                for (int i = 0; i < filterCounts.size(); i++) {
                    Map<String, Object> filterCount = filterCounts.get(i);
                    String value = (String) filterCount.get("group");
                    Integer count = (Integer) filterCount.get("subjects");

                    // Recalculate the count
                    if (thresholds.containsKey(value) && count > thresholds.get(value)) {
                        count = inventoryESService.recountFacetFilterValue(params, RANGE_PARAMS, index, field, value);
                    }

                    // Save the new count
                    newFilterCounts.add(Map.ofEntries(
                        Map.entry("group", value),
                        Map.entry("subjects", count)
                    ));
                }

                // Replace old counts with new counts
                data.put(filterCountQueryName, newFilterCounts);

                // Redo widget counts
                if (widgetQueryName != null) {
                    data.put(widgetQueryName, newFilterCounts);
                }
            }
        }

        caffeineCache.put(cacheKey, data);

        return data;
    }

    private List<Map<String, Object>> cohortCharts(Map<String, Object> params) throws IOException {
        List<Map<String, Object>> chartConfigs = null;
        Object chartConfigsRaw;
        List<Map<String, Object>> charts = new ArrayList<Map<String, Object>>();
        Map<String, Object> cohorts = new HashMap<String, Object>();
        List<String> cohortsCombined = new ArrayList<String>();
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

        if (params == null || !params.containsKey("charts")) {
            return List.of(); // No charts specified
        }

        if (!(params.containsKey("c1") || params.containsKey("c2") || params.containsKey("c3"))) {
            return List.of(); // No cohorts specified
        }

        // Combine cohorts from c1, c2, c3 into a single list
        for (String key : List.of("c1", "c2", "c3")) {
            if (!params.containsKey(key)) {
                continue;
            }

            Object cohortRaw = params.get(key);
            List<String> cohort;

            if (TypeChecker.isOfType(cohortRaw, new TypeToken<List<String>>() {})) {
                @SuppressWarnings("unchecked")
                List<String> castedCohort = (List<String>) cohortRaw;
                cohort = castedCohort;

                if (!cohort.isEmpty()) {
                    // Add cohort to combined list
                    cohortsCombined.addAll(cohort);
                    cohorts.put(key, cohort);
                }
            }
        }

        if (cohortsCombined.isEmpty()) {
            return result;
        }

        chartConfigsRaw = params.get("charts");
        if (TypeChecker.isOfType(chartConfigsRaw, new TypeToken<List<Map<String, Object>>>() {})) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> castedChartConfigs = (List<Map<String, Object>>) chartConfigsRaw;
            chartConfigs = castedChartConfigs;
        }

        if (chartConfigs == null || chartConfigs.isEmpty()) {
            return result;
        }

        // Allocate a map of Opensearch details for each property
        HashMap<String, HashMap<String, String>> groupConfigs = new HashMap<>();
        for (Map<String, Object> chartConfig : chartConfigs) {
            String property = (String) chartConfig.get("property");
            groupConfigs.put(property, new HashMap<String, String>());
        }

        // Retrieve Opensearch details for each property
        for (String index : facetFilters.keySet()) {
            List<Map<String, String>> facetFilterConfigs = facetFilters.get(index);
            for (Map<String, String> facetFilterConfig : facetFilterConfigs) {
                String aggName = facetFilterConfig.get("agg_name");
                if (aggName != null && groupConfigs.containsKey(aggName)) {
                    HashMap<String, String> groupConfig = new HashMap<>(facetFilterConfig);
                    groupConfig.put("index", index);
                    groupConfigs.put(aggName, groupConfig);
                }
            }
        }

        // Generate charts for each configuration
        for (Map<String, Object> chartConfig : chartConfigs) {
            // Prepare map that represents the entire chart
            String property = (String) chartConfig.get("property");
            String type = (String) chartConfig.get("type");
            Map<String, Object> chartData = new HashMap<String, Object>();
            chartData.put("property", property);
            int totalNumberOfParticipants = 0;
            List<String> bucketNames;
            List<String> bucketNamesTopFew;
            List<String> bucketNamesTopMany;

            // Obtain details for querying Opensearch
            Map<String, String> groupConfig = groupConfigs.get(property);
            String cardinalityAggName = groupConfig.get("cardinality_agg_name");
            String endpoint = ENDPOINTS.get(groupConfig.get("index"));
            String indexName = groupConfig.get("index");

            // Determine most populous buckets
            Map<String, Object> combinedCohortParams = Map.of("participant_pk", cohortsCombined);
            bucketNames = inventoryESService.getBucketNames(property, combinedCohortParams, RANGE_PARAMS, cardinalityAggName, indexName, endpoint);

            if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_LOW) {
                bucketNamesTopFew = new ArrayList<>(bucketNames.subList(0, COHORT_CHART_BUCKET_LIMIT_LOW));
            } else {
                bucketNamesTopFew = new ArrayList<>(bucketNames);
            }

            if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_HIGH) {
                bucketNamesTopMany = new ArrayList<>(bucketNames.subList(0, COHORT_CHART_BUCKET_LIMIT_HIGH));
            } else {
                bucketNamesTopMany = new ArrayList<>(bucketNames);
            }

            // If chart type is percentage, then count the total number of participants
            if (type.equals("percentage")) {
                Map<String, Object> combinedCohortsQuery = inventoryESService.buildFacetFilterQuery(combinedCohortParams, RANGE_PARAMS, Set.of(), "participants");

                totalNumberOfParticipants = inventoryESService.getCount(combinedCohortsQuery, "participants");
            }

            // Prepare list of data for each cohort
            List<Map<String, Object>> cohortsData = new ArrayList<Map<String, Object>>();

            // Retrieve data for each cohort
            for (String cohortName : cohorts.keySet()) {
                // Prepare map of data for the cohort
                Map<String, Object> cohortData = new HashMap<String, Object>();
                Map<String, Object> cohortParams = Map.of("participant_pk", cohorts.get(cohortName));
                cohortData.put("cohort", cohortName);

                // Retrieve data for the cohort
                List<Map<String, Object>> cohortGroupCounts = filterSubjectCountBy(property, cohortParams, endpoint, cardinalityAggName, indexName);
                List<Map<String, Object>> cohortGroupCountsTruncated = new ArrayList<Map<String, Object>>();
                int otherMany = 0;
                int otherFew = 0;

                // Format for efficient retrieval
                Map<String, Object> groupsToSubjects = new HashMap<>();
                for (Map<String, Object> groupCount : cohortGroupCounts) {
                    String group = (String) groupCount.get("group");
                    Object subjects = groupCount.get("subjects");
                    groupsToSubjects.put(group, subjects);
                }

                // Add buckets and their counts to a truncated list of results
                for (String bucketName : bucketNames) {
                    Integer subjects = (Integer) groupsToSubjects.getOrDefault(bucketName, 0);

                    if (bucketNamesTopMany.contains(bucketName)) {
                        cohortGroupCountsTruncated.add(Map.of("group", bucketName, "subjects", subjects));

                    } else {
                        otherMany += subjects;
                    }

                    if (!bucketNamesTopFew.contains(bucketName)) {
                        otherFew += subjects;
                    }
                }

                if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_LOW) {
                    cohortGroupCountsTruncated.add(Map.of("group", "OtherFew", "subjects", otherFew));
                }

                if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_HIGH) {
                    cohortGroupCountsTruncated.add(Map.of("group", "OtherMany", "subjects", otherMany));
                }

                // If chart type is percentage, then replace counts with percentages
                if (type.equals("percentage")) {
                    List<Map<String, Object>> cohortGroupPercentages = new ArrayList<Map<String, Object>>();

                    for (Map<String, Object> groupCount : cohortGroupCountsTruncated) {
                        String group = (String) groupCount.get("group");
                        int count = (Integer) groupCount.get("subjects");
                        double percentage = totalNumberOfParticipants > 0 ? ((double) count / totalNumberOfParticipants) * 100 : 0.0;

                        cohortGroupPercentages.add(Map.of("group", group, "subjects", percentage));
                    }

                    cohortData.put("participantsByGroup", cohortGroupPercentages);
                } else if (type.equals("count")) {
                    cohortData.put("participantsByGroup", cohortGroupCountsTruncated);
                }

                // Add cohort data to the list of cohorts
                cohortsData.add(cohortData);
            }

            // Add list of all cohorts' data to the chart
            chartData.put("cohorts", cohortsData);

            // Add chart to the list of charts
            charts.add(chartData);
        }

        return charts;
    }

    private Map<String, String> getGroupConfig(String propertyName) {
        for (String index : facetFilters.keySet()) {
            List<Map<String, String>> groupConfigs = facetFilters.get(index);

            for (Map<String, String> groupConfig : groupConfigs) {
                String aggName = groupConfig.get("agg_name");

                if (aggName != null && aggName.equals(propertyName)) {
                    Map<String, String> modifiedGroupConfig = new HashMap<>(groupConfig);
                    modifiedGroupConfig.put("index", index);
                    return modifiedGroupConfig;
                }
            }
        }

        return null;
    }

    private List<Map<String, Object>> cohortMetadata(Map<String, Object> params) throws IOException {
        List<Map<String, Object>> participants;
        Map<String, Map<String, Map<String, Object>>> consentGroupsByStudy = new HashMap<String, Map<String, Map<String, Object>>>();
        List<Map<String, Object>> listOfStudies = new ArrayList<Map<String, Object>>();

        final List<Map<String, Object>> PROPERTIES = List.of(
            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),

            // Consent Groups
            Map.ofEntries(
                Map.entry("gqlName", "consent_group_name"),
                Map.entry("osName", "consent_group_name")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "consent_group_number"),
                Map.entry("osName", "consent_group_number")
            ),

            // Demographics
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "participant_id"),
                Map.entry("osName", "participant_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "race"),
                Map.entry("osName", "race")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "sex_at_birth"),
                Map.entry("osName", "sex_at_birth")
            ),

            // Diagnoses
            Map.ofEntries(
                Map.entry("gqlName", "diagnoses"),
                Map.entry("osName", "diagnoses")
            ),

            // Genetic Analyses
            Map.ofEntries(
                Map.entry("gqlName", "genetic_analyses"),
                Map.entry("osName", "genetic_analyses")
            ),

            // Survivals
            Map.ofEntries(
                Map.entry("gqlName", "survivals"),
                Map.entry("osName", "survivals")
            ),

            // CPI data
            Map.ofEntries(
                Map.entry("gqlName", "synonyms"),
                Map.entry("osName", "synonyms"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "id"),
                        Map.entry("osName", "id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "associated_id"),
                        Map.entry("osName", "associated_id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "data_location"),
                        Map.entry("osName", "data_location")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "domain_category"),
                        Map.entry("osName", "domain_category")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "domain_description"),
                        Map.entry("osName", "domain_description")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "repository_of_synonym_id"),
                        Map.entry("osName", "repository_of_synonym_id")
                    )
                ))
            ),

            // Treatments
            Map.ofEntries(
                Map.entry("gqlName", "treatments"),
                Map.entry("osName", "treatments")
            ),

            // Treatment Responses
            Map.ofEntries(
                Map.entry("gqlName", "treatment_responses"),
                Map.entry("osName", "treatment_responses")
            )
        );

        String defaultSort = "dbgap_accession"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Studies
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),

            // Consent Groups
            Map.entry("consent_group_name", Map.ofEntries(
                Map.entry("osName", "consent_group_name"),
                Map.entry("isNested", false)
            )),
            Map.entry("consent_group_number", Map.ofEntries(
                Map.entry("osName", "consent_group_number"),
                Map.entry("isNested", false)
            )),

            // Demographics
            Map.entry("participant_pk", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("race", Map.ofEntries(
                Map.entry("osName", "race"),
                Map.entry("isNested", false)
            )),
            Map.entry("sex_at_birth", Map.ofEntries(
                Map.entry("osName", "sex_at_birth"),
                Map.entry("isNested", false)
            )),

            // CPI Data
            Map.entry("synonyms.id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", true),
                Map.entry("path", "synonyms")
            )),
            Map.entry("synonyms.associated_id", Map.ofEntries(
                Map.entry("osName", "associated_id"),
                Map.entry("isNested", true),
                Map.entry("path", "synonyms")
            )),
            Map.entry("synonyms.data_location", Map.ofEntries(
                Map.entry("osName", "data_location"),
                Map.entry("isNested", true),
                Map.entry("path", "synonyms")
            )),
            Map.entry("synonyms.domain_category", Map.ofEntries(
                Map.entry("osName", "domain_category"),
                Map.entry("isNested", true),
                Map.entry("path", "synonyms")
            )),
            Map.entry("synonyms.domain_description", Map.ofEntries(
                Map.entry("osName", "domain_description"),
                Map.entry("isNested", true),
                Map.entry("path", "synonyms")
            )),
            Map.entry("synonyms.repository_of_synonym_id", Map.ofEntries(
                Map.entry("osName", "repository_of_synonym_id"),
                Map.entry("isNested", true),
                Map.entry("path", "synonyms")
            ))
        );

        participants = overview(COHORTS_END_POINT, params, PROPERTIES, defaultSort, mapping, "participants");

        // Group participants by consent group and then by study
        participants.forEach((Map<String, Object> participant) -> {
            String dbgapAccession = (String) participant.get("dbgap_accession");
            String consentGroupName = (String) participant.get("consent_group_name");
            String consentGroupNumber = (String) participant.get("consent_group_number");

            // Make sure a mapping exists for the study
            if (!consentGroupsByStudy.containsKey(dbgapAccession)) {
                consentGroupsByStudy.put(dbgapAccession, new HashMap<String, Map<String, Object>>());
            }

            // Make sure a mapping exists for the consent group
            if (!consentGroupsByStudy.get(dbgapAccession).containsKey(consentGroupName)) {
                Map<String, Object> consentGroup = new HashMap<String, Object>();

                consentGroup.put("consent_group_name", consentGroupName);
                consentGroup.put("consent_group_number", consentGroupNumber);
                consentGroup.put("participants", new ArrayList<Map<String, Object>>());
                consentGroupsByStudy.get(dbgapAccession).put(consentGroupName, consentGroup);
            }

            // Add to the consent group's list of participants
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> participantsList = (List<Map<String, Object>>) consentGroupsByStudy.get(dbgapAccession).get(consentGroupName).get("participants");
            participantsList.add(participant);
        });

        // Structure a list of studies to return
        // Study->Consent Group->Participant
        consentGroupsByStudy.forEach((accession, consentGroups) -> {
            listOfStudies.add(Map.ofEntries(
                Map.entry("dbgap_accession", accession),
                Map.entry("consent_groups", consentGroups.values())
            ));
        });

        return listOfStudies;
    }

    private List<Map<String, Object>> participantOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Demographics
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "participant_id"),
                Map.entry("osName", "participant_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "race"),
                Map.entry("osName", "race")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "sex_at_birth"),
                Map.entry("osName", "sex_at_birth")
            ),

            // CPI Data
            Map.ofEntries(
                Map.entry("gqlName", "cpi_data"),
                Map.entry("osName", "cpi_data"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "associated_id"),
                        Map.entry("osName", "associated_id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "data_location"),
                        Map.entry("osName", "data_location")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "data_type"),
                        Map.entry("osName", "data_type")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "domain_category"),
                        Map.entry("osName", "domain_category")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "domain_description"),
                        Map.entry("osName", "domain_description")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "repository_of_synonym_id"),
                        Map.entry("osName", "repository_of_synonym_id")
                    )
                ))
            ),

            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),

            // Additional fields for download
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            )
        );

        String defaultSort = "participant_id"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(// field -> sort field
            // Demographics
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("race", Map.ofEntries(
                Map.entry("osName", "race_str"),
                Map.entry("isNested", false)
            )),
            Map.entry("sex_at_birth", Map.ofEntries(
                Map.entry("osName", "sex_at_birth"),
                Map.entry("isNested", false)
            )),

            // CPI Data
            Map.entry("cpi_data", Map.ofEntries(
                Map.entry("osName", "cpi_data"),
                Map.entry("isNested", false)
            )),

            // Studies
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),

            // Additional fields for download
            Map.entry("study_id", Map.ofEntries(
                Map.entry("osName", "study_id"),
                Map.entry("isNested", false)
            ))
        );

        return overview(PARTICIPANTS_END_POINT, params, PROPERTIES, defaultSort, mapping, "participants");
    }

    private List<Map<String, Object>> diagnosisOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Demographics
            Map.ofEntries(
                Map.entry("gqlName", "participant"),
                Map.entry("osName", "participant"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "id"),
                        Map.entry("osName", "id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "participant_id"),
                        Map.entry("osName", "participant_id")
                    ),

                    // Additional fields for Cohort manifest download
                    Map.ofEntries(
                        Map.entry("gqlName", "race"),
                        Map.entry("osName", "race")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "sex_at_birth"),
                        Map.entry("osName", "sex_at_birth")
                    )
                ))
            ),

            // Diagnoses
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "age_at_diagnosis"),
                Map.entry("osName", "age_at_diagnosis_str")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "anatomic_site"),
                Map.entry("osName", "anatomic_site")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "diagnosis_basis"),
                Map.entry("osName", "diagnosis_basis")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "diagnosis"),
                Map.entry("osName", "diagnosis")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "diagnosis_classification_system"),
                Map.entry("osName", "diagnosis_classification_system")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "disease_phase"),
                Map.entry("osName", "disease_phase")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "tumor_classification"),
                Map.entry("osName", "tumor_classification")
            ),

            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),


            // Additional fields for download
            Map.ofEntries(
                Map.entry("gqlName", "diagnosis_id"),
                Map.entry("osName", "diagnosis_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "diagnosis_comment"),
                Map.entry("osName", "diagnosis_comment")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "toronto_childhood_cancer_staging"),
                Map.entry("osName", "toronto_childhood_cancer_staging")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "tumor_grade"),
                Map.entry("osName", "tumor_grade")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "tumor_stage_clinical_m"),
                Map.entry("osName", "tumor_stage_clinical_m")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "tumor_stage_clinical_n"),
                Map.entry("osName", "tumor_stage_clinical_n")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "tumor_stage_clinical_t"),
                Map.entry("osName", "tumor_stage_clinical_t")
            )
        );

        String defaultSort = "diagnosis_id"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Diagnoses
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("age_at_diagnosis", Map.ofEntries(
                Map.entry("osName", "age_at_diagnosis"),
                Map.entry("isNested", false)
            )),
            Map.entry("anatomic_site", Map.ofEntries(
                Map.entry("osName", "anatomic_site"),
                Map.entry("isNested", false)
            )),
            Map.entry("diagnosis_basis", Map.ofEntries(
                Map.entry("osName", "diagnosis_basis"),
                Map.entry("isNested", false)
            )),
            Map.entry("diagnosis", Map.ofEntries(
                Map.entry("osName", "diagnosis"),
                Map.entry("isNested", false)
            )),
            Map.entry("diagnosis_classification_system", Map.ofEntries(
                Map.entry("osName", "diagnosis_classification_system"),
                Map.entry("isNested", false)
            )),
            Map.entry("disease_phase", Map.ofEntries(
                Map.entry("osName", "disease_phase"),
                Map.entry("isNested", false)
            )),
            Map.entry("tumor_classification", Map.ofEntries(
                Map.entry("osName", "tumor_classification"),
                Map.entry("isNested", false)
            )),

            // Demographics
            Map.entry("participant.participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", true),
                Map.entry("path", "participant")
            )),

            // Studies
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),

            // Additional fields for download
            Map.entry("diagnosis_id", Map.ofEntries(
                Map.entry("osName", "diagnosis_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("diagnosis_comment", Map.ofEntries(
                Map.entry("osName", "diagnosis_comment"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_id", Map.ofEntries(
                Map.entry("osName", "study_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("toronto_childhood_cancer_staging", Map.ofEntries(
                Map.entry("osName", "toronto_childhood_cancer_staging"),
                Map.entry("isNested", false)
            )),
            Map.entry("tumor_grade", Map.ofEntries(
                Map.entry("osName", "tumor_grade"),
                Map.entry("isNested", false)
            )),
            Map.entry("tumor_stage_clinical_m", Map.ofEntries(
                Map.entry("osName", "tumor_stage_clinical_m"),
                Map.entry("isNested", false)
            )),
            Map.entry("tumor_stage_clinical_n", Map.ofEntries(
                Map.entry("osName", "tumor_stage_clinical_n"),
                Map.entry("isNested", false)
            )),
            Map.entry("tumor_stage_clinical_t", Map.ofEntries(
                Map.entry("osName", "tumor_stage_clinical_t"),
                Map.entry("isNested", false)
            ))
        );

        return overview(DIAGNOSES_END_POINT, params, PROPERTIES, defaultSort, mapping, "diagnoses");
    }

    private List<Map<String, Object>> geneticAnalysisOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Study
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),

            // Genetic Analysis
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "genetic_analysis_id"),
                Map.entry("osName", "genetic_analysis_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "alteration"),
                Map.entry("osName", "alteration")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "cytoband"),
                Map.entry("osName", "cytoband")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "gene_symbol"),
                Map.entry("osName", "gene_symbol")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "genomic_source_category"),
                Map.entry("osName", "genomic_source_category")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "hgvs_coding"),
                Map.entry("osName", "hgvs_coding")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "hgvs_genome"),
                Map.entry("osName", "hgvs_genome")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "hgvs_protein"),
                Map.entry("osName", "hgvs_protein")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "status"),
                Map.entry("osName", "status")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "test"),
                Map.entry("osName", "test")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "reported_significance"),
                Map.entry("osName", "reported_significance")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "reported_significance_system"),
                Map.entry("osName", "reported_significance_system")
            ),

            // Demographics
            Map.ofEntries(
                Map.entry("gqlName", "participant"),
                Map.entry("osName", "participant"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "id"),
                        Map.entry("osName", "id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "participant_id"),
                        Map.entry("osName", "participant_id")
                    ),

                    // Additional fields for Cohort manifest download
                    Map.ofEntries(
                        Map.entry("gqlName", "race"),
                        Map.entry("osName", "race")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "sex_at_birth"),
                        Map.entry("osName", "sex_at_birth")
                    )
                ))
            ),

            // Additional fields for download
            Map.ofEntries(
                Map.entry("gqlName", "alteration_effect"),
                Map.entry("osName", "alteration_effect")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "alteration_type"),
                Map.entry("osName", "alteration_type")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "chromosome"),
                Map.entry("osName", "chromosome")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "exon"),
                Map.entry("osName", "exon")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "fusion_partner_exon"),
                Map.entry("osName", "fusion_partner_exon")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "fusion_partner_gene"),
                Map.entry("osName", "fusion_partner_gene")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "reference_genome"),
                Map.entry("osName", "reference_genome")
            )
        );

        String defaultSort = "genetic_analysis_id"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Study
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),

            // Genetic Analysis
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("genetic_analysis_id", Map.ofEntries(
                Map.entry("osName", "genetic_analysis_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("alteration", Map.ofEntries(
                Map.entry("osName", "alteration"),
                Map.entry("isNested", false)
            )),
            Map.entry("cytoband", Map.ofEntries(
                Map.entry("osName", "cytoband"),
                Map.entry("isNested", false)
            )),
            Map.entry("gene_symbol", Map.ofEntries(
                Map.entry("osName", "gene_symbol"),
                Map.entry("isNested", false)
            )),
            Map.entry("genomic_source_category", Map.ofEntries(
                Map.entry("osName", "genomic_source_category"),
                Map.entry("isNested", false)
            )),
            Map.entry("hgvs_coding", Map.ofEntries(
                Map.entry("osName", "hgvs_coding"),
                Map.entry("isNested", false)
            )),
            Map.entry("hgvs_genome", Map.ofEntries(
                Map.entry("osName", "hgvs_genome"),
                Map.entry("isNested", false)
            )),
            Map.entry("hgvs_protein", Map.ofEntries(
                Map.entry("osName", "hgvs_protein"),
                Map.entry("isNested", false)
            )),
            Map.entry("status", Map.ofEntries(
                Map.entry("osName", "status"),
                Map.entry("isNested", false)
            )),
            Map.entry("test", Map.ofEntries(
                Map.entry("osName", "test"),
                Map.entry("isNested", false)
            )),
            Map.entry("reported_significance", Map.ofEntries(
                Map.entry("osName", "reported_significance"),
                Map.entry("isNested", false)
            )),
            Map.entry("reported_significance_system", Map.ofEntries(
                Map.entry("osName", "reported_significance_system"),
                Map.entry("isNested", false)
            )),

            // Demographics
            Map.entry("participant.participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", true),
                Map.entry("path", "participant")
            )),

            // Additional fields for download
            Map.entry("alteration_effect", Map.ofEntries(
                Map.entry("osName", "alteration_effect"),
                Map.entry("isNested", false)
            )),
            Map.entry("alteration_type", Map.ofEntries(
                Map.entry("osName", "alteration_type"),
                Map.entry("isNested", false)
            )),
            Map.entry("chromosome", Map.ofEntries(
                Map.entry("osName", "chromosome"),
                Map.entry("isNested", false)
            )),
            Map.entry("exon", Map.ofEntries(
                Map.entry("osName", "exon"),
                Map.entry("isNested", false)
            )),
            Map.entry("fusion_partner_exon", Map.ofEntries(
                Map.entry("osName", "fusion_partner_exon"),
                Map.entry("isNested", false)
            )),
            Map.entry("fusion_partner_gene", Map.ofEntries(
                Map.entry("osName", "fusion_partner_gene"),
                Map.entry("isNested", false)
            )),
            Map.entry("reference_genome", Map.ofEntries(
                Map.entry("osName", "reference_genome"),
                Map.entry("isNested", false)
            ))
        );

        return overview(GENETIC_ANALYSES_END_POINT, params, PROPERTIES, defaultSort, mapping, "genetic_analyses");
    }

    private List<Map<String, Object>> studyOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_name"),
                Map.entry("osName", "study_name")
            ),

            // Additional fields for download
            Map.ofEntries(
                Map.entry("gqlName", "external_url"),
                Map.entry("osName", "external_url")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_description"),
                Map.entry("osName", "study_description")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            )
        );

        String defaultSort = "dbgap_accession"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Studies
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_name", Map.ofEntries(
                Map.entry("osName", "study_name"),
                Map.entry("isNested", false)
            )),

            // Additional fields for download
            Map.entry("external_url", Map.ofEntries(
                Map.entry("osName", "external_url"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_description", Map.ofEntries(
                Map.entry("osName", "study_description"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_id", Map.ofEntries(
                Map.entry("osName", "study_id"),
                Map.entry("isNested", false)
            ))
        );
        
        Request request = new Request("GET", PARTICIPANTS_END_POINT);
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), "participants");
        String[] AGG_NAMES = new String[] {"study_id"};
        query = inventoryESService.addAggregations(query, AGG_NAMES);
        String queryJson = gson.toJson(query);
        request.setJsonEntity(queryJson);
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectTermAggs(jsonObject, AGG_NAMES);
        JsonArray buckets = aggs.get("study_id");
        List<String> data = new ArrayList<>();
        for (var bucket: buckets) {
            data.add(bucket.getAsJsonObject().get("key").getAsString());
        }

        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        
        Map<String, Object> study_params = new HashMap<>();
        if (data.size() == 0) {
            data.add("-1");
        }
        study_params.put("study_id", data);
        study_params.put(ORDER_BY, order_by);
        study_params.put(SORT_DIRECTION, direction);
        study_params.put(PAGE_SIZE, pageSize);
        study_params.put(OFFSET, offset);

        return overview(STUDIES_END_POINT, study_params, PROPERTIES, defaultSort, mapping, "studies");
    }

    private List<Map<String, Object>> survivalOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Participants
            Map.ofEntries(
                Map.entry("gqlName", "participant"),
                Map.entry("osName", "participant"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "id"),
                        Map.entry("osName", "id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "participant_id"),
                        Map.entry("osName", "participant_id")
                    ),

                    // Additional fields for Cohort manifest download
                    Map.ofEntries(
                        Map.entry("gqlName", "race"),
                        Map.entry("osName", "race")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "sex_at_birth"),
                        Map.entry("osName", "sex_at_birth")
                    )
                ))
            ),

            // Survivals
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),

            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "age_at_last_known_survival_status"),
                Map.entry("osName", "age_at_last_known_survival_status_str")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "cause_of_death"),
                Map.entry("osName", "cause_of_death")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "first_event"),
                Map.entry("osName", "first_event")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "last_known_survival_status"),
                Map.entry("osName", "last_known_survival_status")
            ),

            // Additional fields for download
            Map.ofEntries(
                Map.entry("gqlName", "age_at_event_free_survival_status"),
                Map.entry("osName", "age_at_event_free_survival_status_str")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "event_free_survival_status"),
                Map.entry("osName", "event_free_survival_status")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "survival_id"),
                Map.entry("osName", "survival_id")
            )
        );

        String defaultSort = "survival_id"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Participants
            Map.entry("participant.participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", true),
                Map.entry("path", "participant")
            )),

            // Studies
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),

            // Survivals
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("age_at_last_known_survival_status", Map.ofEntries(
                Map.entry("osName", "age_at_last_known_survival_status"),
                Map.entry("isNested", false)
            )),
            Map.entry("cause_of_death", Map.ofEntries(
                Map.entry("osName", "cause_of_death"),
                Map.entry("isNested", false)
            )),
            Map.entry("first_event", Map.ofEntries(
                Map.entry("osName", "first_event"),
                Map.entry("isNested", false)
            )),
            Map.entry("last_known_survival_status", Map.ofEntries(
                Map.entry("osName", "last_known_survival_status"),
                Map.entry("isNested", false)
            )),

            // Additional fields for download
            Map.entry("age_at_event_free_survival_status", Map.ofEntries(
                Map.entry("osName", "age_at_event_free_survival_status"),
                Map.entry("isNested", false)
            )),
            Map.entry("event_free_survival_status", Map.ofEntries(
                Map.entry("osName", "event_free_survival_status"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_id", Map.ofEntries(
                Map.entry("osName", "study_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("survival_id", Map.ofEntries(
                Map.entry("osName", "survival_id"),
                Map.entry("isNested", false)
            ))
        );

        return overview(SURVIVALS_END_POINT, params, PROPERTIES, defaultSort, mapping, "survivals");
    }

    private List<Map<String, Object>> treatmentOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Participants
            Map.ofEntries(
                Map.entry("gqlName", "participant"),
                Map.entry("osName", "participant"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "id"),
                        Map.entry("osName", "id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "participant_id"),
                        Map.entry("osName", "participant_id")
                    ),

                    // Additional fields for Cohort manifest download
                    Map.ofEntries(
                        Map.entry("gqlName", "race"),
                        Map.entry("osName", "race")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "sex_at_birth"),
                        Map.entry("osName", "sex_at_birth")
                    )
                ))
            ),

            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            ),

            // Treatments
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "treatment_id"),
                Map.entry("osName", "treatment_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "age_at_treatment_start"),
                Map.entry("osName", "age_at_treatment_start_str")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "age_at_treatment_end"),
                Map.entry("osName", "age_at_treatment_end_str")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "treatment_type"),
                Map.entry("osName", "treatment_type")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "treatment_agent"),
                Map.entry("osName", "treatment_agent")
            )
        );

        String defaultSort = "treatment_type"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Participants
            Map.entry("participant.participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", true),
                Map.entry("path", "participant")
            )),

            // Studies
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_id", Map.ofEntries(
                Map.entry("osName", "study_id"),
                Map.entry("isNested", false)
            )),

            // Treatments
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("treatment_id", Map.ofEntries(
                Map.entry("osName", "treatment_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("age_at_treatment_start", Map.ofEntries(
                Map.entry("osName", "age_at_treatment_start"),
                Map.entry("isNested", false)
            )),
            Map.entry("age_at_treatment_end", Map.ofEntries(
                Map.entry("osName", "age_at_treatment_end"),
                Map.entry("isNested", false)
            )),
            Map.entry("treatment_type", Map.ofEntries(
                Map.entry("osName", "treatment_type"),
                Map.entry("isNested", false)
            )),
            Map.entry("treatment_agent", Map.ofEntries(
                Map.entry("osName", "treatment_agent_str"),
                Map.entry("isNested", false)
            ))
        );

        return overview(TREATMENTS_END_POINT, params, PROPERTIES, defaultSort, mapping, "treatments");
    }

    private List<Map<String, Object>> treatmentResponseOverview(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> PROPERTIES = List.of(
            // Participants
            Map.ofEntries(
                Map.entry("gqlName", "participant"),
                Map.entry("osName", "participant"),
                Map.entry("nested", List.of(
                    Map.ofEntries(
                        Map.entry("gqlName", "id"),
                        Map.entry("osName", "id")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "participant_id"),
                        Map.entry("osName", "participant_id")
                    ),

                    // Additional fields for Cohort manifest download
                    Map.ofEntries(
                        Map.entry("gqlName", "race"),
                        Map.entry("osName", "race")
                    ),
                    Map.ofEntries(
                        Map.entry("gqlName", "sex_at_birth"),
                        Map.entry("osName", "sex_at_birth")
                    )
                ))
            ),

            // Studies
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            ),

            // Treatment Responses
            Map.ofEntries(
                Map.entry("gqlName", "id"),
                Map.entry("osName", "id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "treatment_response_id"),
                Map.entry("osName", "treatment_response_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "response"),
                Map.entry("osName", "response")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "age_at_response"),
                Map.entry("osName", "age_at_response_str")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "response_category"),
                Map.entry("osName", "response_category")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "response_system"),
                Map.entry("osName", "response_system")
            )
        );

        String defaultSort = "response"; // Default sort order

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            // Participants
            Map.entry("participant.participant_id", Map.ofEntries(
                Map.entry("osName", "participant_id"),
                Map.entry("isNested", true),
                Map.entry("path", "participant")
            )),

            // Studies
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_id", Map.ofEntries(
                Map.entry("osName", "study_id"),
                Map.entry("isNested", false)
            )),

            // Treatment Responses
            Map.entry("id", Map.ofEntries(
                Map.entry("osName", "id"),
                Map.entry("isNested", false)
            )),
            Map.entry("treatment_response_id", Map.ofEntries(
                Map.entry("osName", "treatment_response_id"),
                Map.entry("isNested", false)
            )),
            Map.entry("response", Map.ofEntries(
                Map.entry("osName", "response"),
                Map.entry("isNested", false)
            )),
            Map.entry("age_at_response", Map.ofEntries(
                Map.entry("osName", "age_at_response"),
                Map.entry("isNested", false)
            )),
            Map.entry("response_category", Map.ofEntries(
                Map.entry("osName", "response_category"),
                Map.entry("isNested", false)
            )),
            Map.entry("response_system", Map.ofEntries(
                Map.entry("osName", "response_system"),
                Map.entry("isNested", false)
            ))
        );

        return overview(TREATMENT_RESPONSES_END_POINT, params, PROPERTIES, defaultSort, mapping, "treatment_responses");
    }

    /**
     * Returns a list of records that match the given filters
     * @param endpoint The Opensearch endpoint to query
     * @param params The GraphQL variables to filter by
     * @param properties The properties to retrieve
     * @param defaultSort The default sort
     * @param mapping Map of how to sort each field
     * @param overviewType The type of records retrieved
     * @return
     * @throws IOException
     */
    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, List<Map<String, Object>> properties, String defaultSort, Map<String, Map<String, Object>> mapping, String overviewType) throws IOException {
        Request request = new Request("GET", endpoint);
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), overviewType);
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        List<Map<String, Object>> page = inventoryESService.collectPage(request, query, properties, pageSize, offset);
        return page;
    }

    private Map<String, Object> studyDetails(Map<String, Object> params) throws IOException {
        Map<String, Object> study;
        String studyId = (String) params.get("study_id");
        List<Map<String, Object>> studies;

        final List<Map<String, Object>> PROPERTIES = List.of(
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_description"),
                Map.entry("osName", "study_description")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "num_participants"),
                Map.entry("osName", "num_participants")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "num_diseases"),
                Map.entry("osName", "num_diseases")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "num_anatomic_sites"),
                Map.entry("osName", "num_anatomic_sites")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "num_survivals"),
                Map.entry("osName", "num_survivals")
            )
        );

        Map<String, Map<String, Object>> mapping = Map.ofEntries(
            Map.entry("dbgap_accession", Map.ofEntries(
                Map.entry("osName", "dbgap_accession"),
                Map.entry("isNested", false)
            )),
            Map.entry("study_description", Map.ofEntries(
                Map.entry("osName", "study_description"),
                Map.entry("isNested", false)
            )),
            Map.entry("num_participants", Map.ofEntries(
                Map.entry("osName", "num_participants"),
                Map.entry("isNested", false)
            )),
            Map.entry("num_diseases", Map.ofEntries(
                Map.entry("osName", "num_diseases"),
                Map.entry("isNested", false)
            )),
            Map.entry("num_anatomic_sites", Map.ofEntries(
                Map.entry("osName", "num_anatomic_sites"),
                Map.entry("isNested", false)
            )),
            Map.entry("num_survivals", Map.ofEntries(
                Map.entry("osName", "num_survivals"),
                Map.entry("isNested", false)
            ))
        );

        Map<String, Object> study_params = Map.ofEntries(
            Map.entry("dbgap_accession", List.of(studyId)),
            Map.entry(ORDER_BY, "dbgap_accession"),
            Map.entry(SORT_DIRECTION, "ASC"),
            Map.entry(PAGE_SIZE, 1),
            Map.entry(OFFSET, 0)
        );

        studies = overview(STUDIES_END_POINT, study_params, PROPERTIES, "dbgap_accession", mapping, "studies");

        try {
            study = studies.get(0);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }

        return study;
    }

    private List<Map<String, Object>> studiesListing() throws IOException {
        List<Map<String, Object>> properties = List.of(
            Map.ofEntries(
                Map.entry("gqlName", "dbgap_accession"),
                Map.entry("osName", "dbgap_accession")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_name"),
                Map.entry("osName", "study_name")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "num_participants"),
                Map.entry("osName", "num_participants")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "num_diseases"),
                Map.entry("osName", "num_diseases")
            )
        );

        Map<String, Object> query = esService.buildListQuery();
        Request request = new Request("GET", STUDIES_END_POINT);
        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }

    private List<Map<String, Object>> findParticipantIdsInList(Map<String, Object> params) throws IOException {
        final List<Map<String, Object>> properties = List.of(
            Map.ofEntries(
                Map.entry("gqlName", "participant_id"),
                Map.entry("osName", "participant_id")
            ),
            Map.ofEntries(
                Map.entry("gqlName", "study_id"),
                Map.entry("osName", "study_id")
            )
        );

        Map<String, Object> query = esService.buildListQuery(params, Set.of(), false);
        Request request = new Request("GET",PARTICIPANTS_END_POINT);

        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }

    private Map<String, Object> mapSortOrder(String order_by, String direction, String defaultSort, Map<String, Map<String, Object>> mapping) {
        String sortDirection = "asc";
        Object sortPredicate;

        // Handle invalid sort parameters
        if (!mapping.containsKey(order_by)) {
            logger.info("Order: \"" + order_by + "\" not recognized, use default order");
            return Map.of(defaultSort, sortDirection);
        }

        // Only two valid sort directions
        if (sortDirection.equalsIgnoreCase("asc") || sortDirection.equalsIgnoreCase("desc")) {
            sortDirection = direction;
        }

        Map<String, Object> mappingDetails = mapping.get(order_by);
        boolean isNested = (Boolean) mappingDetails.get("isNested");
        String propName = (String) mappingDetails.get("osName");

        if (isNested) {
            String nestedPath = (String) mappingDetails.get("path");
            propName = String.join(".", nestedPath, propName);
            sortPredicate = Map.ofEntries(
                Map.entry("nested_path", nestedPath),
                Map.entry("order", sortDirection)
            );
        } else {
            sortPredicate = sortDirection;
        }

        return Map.of(propName, sortPredicate);
    }

    private Integer numberOfDiseases(Map<String, Object> params) throws Exception {
        // String cacheKey = generateCacheKey(params);
        // Integer data = (Integer)caffeineCache.asMap().get(cacheKey);

        // if (data != null) {
        //     logger.info("hit cache!");
        //     return data;
        // }

        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = inventoryESService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_diseases").getAsInt();

        // caffeineCache.put(cacheKey, data);

        return count;
    }

    private Integer numberOfParticipants(Map<String, Object> params) throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = inventoryESService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_participants").getAsInt();

        return count;
    }

    private Integer numberOfReferenceFiles(Map<String, Object> params) throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = inventoryESService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_reference_files").getAsInt();

        return count;
    }

    private Integer numberOfStudies(Map<String, Object> params) throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = inventoryESService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_studies").getAsInt();

        return count;
    }

    private Integer numberOfSurvivals(Map<String, Object> params) throws Exception {
        Request homeStatsRequest = new Request("GET", HOME_STATS_END_POINT);
        JsonObject homeStatsResult = inventoryESService.send(homeStatsRequest);
        JsonArray hits = homeStatsResult.getAsJsonObject("hits").getAsJsonArray("hits");
        Iterator<JsonElement> hitsIter = hits.iterator();

        if (!hitsIter.hasNext()) {
            throw new Exception("Error: no results for homepage stats!");
        }

        JsonObject counts = hitsIter.next().getAsJsonObject().getAsJsonObject("_source");
        int count = counts.get("num_survivals").getAsInt();

        return count;
    }

    private String generateCacheKey(Map<String, Object> params) throws IOException {
        List<String> keys = new ArrayList<>();
        for (String key: params.keySet()) {
            if (RANGE_PARAMS.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Integer> bounds = null;
                Object boundsRaw = params.get(key);

                if (TypeChecker.isOfType(boundsRaw, new TypeToken<List<Integer>>() {})) {
                    @SuppressWarnings("unchecked")
                    List<Integer> castedBounds = (List<Integer>) boundsRaw;
                    bounds = castedBounds;
                }

                if (bounds.size() >= 2) {
                    Integer lower = bounds.get(0);
                    Integer higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    keys.add(key.concat(lower.toString()).concat(higher.toString()));
                }
            } else {
                List<String> valueSet = null;
                Object valueSetRaw = params.get(key);

                if (TypeChecker.isOfType(valueSetRaw, new TypeToken<List<String>>() {})) {
                    @SuppressWarnings("unchecked")
                    List<String> castedValueSet = (List<String>) valueSetRaw;
                    valueSet = castedValueSet;
                }
            
                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    keys.add(key.concat(valueSet.toString()));
                }
            }
        }
        if (keys.size() == 0){
            return "all";
        } else {
            return keys.toString();
        }
    }
}
