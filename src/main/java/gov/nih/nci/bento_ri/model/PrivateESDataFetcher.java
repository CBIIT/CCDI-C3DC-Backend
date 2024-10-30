package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.InputStream;
import java.io.IOException;
import java.util.*;

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

    final String STUDIES_FACET_END_POINT = "/study_participants/_search";
    final String COHORTS_END_POINT = "/cohorts/_search";
    final String PARTICIPANTS_END_POINT = "/participants/_search";
    final String SURVIVALS_END_POINT = "/survivals/_search";
    final String TREATMENTS_END_POINT = "/treatments/_search";
    final String TREATMENT_RESPONSES_END_POINT = "/treatment_responses/_search";
    final String DIAGNOSES_END_POINT = "/diagnoses/_search";
    final String HOME_STATS_END_POINT = "/home_stats/_search";
    final String STUDIES_END_POINT = "/studies/_search";
    final String SAMPLES_END_POINT = "/samples/_search";
    final Map<String, String> ENDPOINTS = Map.ofEntries(
        Map.entry("diagnoses", DIAGNOSES_END_POINT),
        Map.entry("participants", PARTICIPANTS_END_POINT),
        Map.entry("studies", STUDIES_END_POINT),
        Map.entry("survivals", SURVIVALS_END_POINT),
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
        List<String> valueSet = INCLUDE_PARAMS.contains(category) ? (List<String>)params.get(category) : List.of();
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
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectRangCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return getGroupCountHelper(buckets, cardinalityAggName);
    }

    private List<Map<String, Object>> getGroupCount(String category, Map<String, Object> query, String endpoint, String cardinalityAggName, List<String> only_includes) throws IOException {
        if (RANGE_PARAMS.contains(category)) {
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
            request.setJsonEntity(gson.toJson(query));
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

    private List<Map<String, Object>> getBooleanGroupCountHelper(JsonObject filters) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map.Entry<String, JsonElement> group: filters.entrySet()) {
            int count = group.getValue().getAsJsonObject().get("parent").getAsJsonObject().get("doc_count").getAsInt();
            if (count > 0) {
                data.add(Map.of("group", group.getKey(),
                    "subjects", count
                ));
            }
        }
        return data;
    }

    private List<Map<String, Object>> getGroupCountHelper(JsonArray buckets, String cardinalityAggName) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (JsonElement group: buckets) {
            data.add(Map.of("group", group.getAsJsonObject().get("key").getAsString(),
                    "subjects", !(cardinalityAggName == null) ? group.getAsJsonObject().get("cardinality_count").getAsJsonObject().get("value").getAsInt() : group.getAsJsonObject().get("doc_count").getAsInt()
            ));

        }
        return data;
    }

    private Map<String, String[]> idsLists() throws IOException {
        // Specify which Opensearch fields to obtain GraphQL return values from
        Map<String, String[][]> indexProperties = Map.ofEntries(
            Map.entry(PARTICIPANTS_END_POINT, new String[][]{
                new String[]{"participantIds", "participant_id"}
            })
        );
        // Define sort priorty, from highest to lowest
        Map<String, String[]> sortPriority = Map.ofEntries(
            Map.entry(PARTICIPANTS_END_POINT, new String[]{
                "participant_id"
            })
        );
        //Generic Query
        Map<String, Object> query = esService.buildListQuery();
        //Results Map
        Map<String, String[]> results = new HashMap<>();
        //Iterate through each index properties map and make a request to each endpoint then format the results as
        // String arrays
        String cacheKey = "participantIDs";
        Map<String, String[]> data = (Map<String, String[]>)caffeineCache.asMap().get(cacheKey);
        if (data != null) {
            logger.info("hit cache!");
            return data;
        }

        for (String endpoint: indexProperties.keySet()){
            Request request = new Request("GET", endpoint);
            String[][] properties = indexProperties.get(endpoint);
            String[] sortOrder = sortPriority.get(endpoint);
            ArrayList<Map<String, String>> sortParams = new ArrayList<Map<String, String>>();
            List<String> fields = new ArrayList<>();

            for (String[] prop: properties) {
                fields.add(prop[1]);
            }

            for (String sortField: sortOrder) {
                sortParams.add(Map.of(sortField, "asc"));
            }
            
            query.put("_source", fields);
            query.put("sort", sortParams);

            List<Map<String, Object>> result = esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE,
                    0);
            Map<String, List<String>> indexResults = new HashMap<>();
            Arrays.asList(properties).forEach(x -> indexResults.put(x[0], new ArrayList<>()));
            for(Map<String, Object> resultElement: result){
                for(String key: indexResults.keySet()){
                    List<String> tmp = indexResults.get(key);
                    String v = (String) resultElement.get(key);
                    if (!tmp.contains(v)) {
                        tmp.add(v);
                    }
                }
            }
            for(String key: indexResults.keySet()){
                results.put(key, indexResults.get(key).toArray(new String[indexResults.size()]));
            }
        }
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
        Map<String, Object> data = (Map<String, Object>)caffeineCache.asMap().get(cacheKey);

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

                // Get widget counts
                if (widgetQueryName != null) {
                    // Fetch data for widgets
                    if (RANGE_PARAMS.contains(field)) {
                        List<Map<String, Object>> subjectCount = subjectCountByRange(field, params, endpoint, cardinalityAggName, index);
                        data.put(widgetQueryName, subjectCount);
                    } else if (params.containsKey(field) && ((List<String>) params.get(field)).size() > 0) {
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

    private List<Map<String, Object>> cohortMetadata(Map<String, Object> params) throws IOException {
        List<Map<String, Object>> participants;
        Map<String, List<Map<String, Object>>> participantsByStudy = new HashMap<String, List<Map<String, Object>>>();
        List<Map<String, Object>> listOfParticipantsByStudy = new ArrayList<Map<String, Object>>();

        final String[][] PROPERTIES = new String[][]{
            // Studies
            new String[]{"dbgap_accession", "dbgap_accession"},

            // Demographics
            new String[]{"participant_pk", "participant_pk"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"race", "race"},
            new String[]{"sex_at_birth", "sex_at_birth"},

            // Diagnoses
            new String[]{"diagnoses", "diagnoses"},

            // Survivals
            new String[]{"survivals", "survivals"},

            // Treatments
            new String[]{"treatments", "treatments"},

            // Treatment Responses
            new String[]{"treatment_responses", "treatment_responses"},
        };

        String defaultSort = "dbgap_accession"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Studies
            Map.entry("dbgap_accession", "dbgap_accession"),

            // Demographics
            Map.entry("participant_pk", "participant_pk"),
            Map.entry("participant_id", "participant_id"),
            Map.entry("race", "race"),
            Map.entry("sex_at_birth", "sex_at_birth")
        );

        participants = overview(COHORTS_END_POINT, params, PROPERTIES, defaultSort, mapping, "participants");

        // Restructure the data to a map, keyed by dbgap_accession
        participants.forEach((Map<String, Object> participant) -> {
            String dbgapAccession = (String) participant.get("dbgap_accession");

            if (participantsByStudy.containsKey(dbgapAccession)) {
                participantsByStudy.get(dbgapAccession).add(participant);
            } else {
                participantsByStudy.put(dbgapAccession, new ArrayList<Map<String, Object>>(
                    List.of(participant)
                ));
            }
        });

        // Restructure the map to a list
        participantsByStudy.forEach((accession, people) -> {
            listOfParticipantsByStudy.add(Map.ofEntries(
                Map.entry("dbgap_accession", accession),
                Map.entry("participants", people)
            ));
        });

        return listOfParticipantsByStudy;
    }

    private List<Map<String, Object>> participantOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Demographics
            new String[]{"participant_pk", "participant_pk"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"race", "race_str"},
            new String[]{"sex_at_birth", "sex_at_birth"},

            // Studies
            new String[]{"dbgap_accession", "dbgap_accession"},

            // Additional fields for download
            new String[]{"study_id", "study_id"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Demographics
            Map.entry("participant_pk", "participant_pk"),
            Map.entry("participant_id", "participant_id"),
            Map.entry("race", "race_str"),
            Map.entry("sex_at_birth", "sex_at_birth"),

            // Studies
            Map.entry("dbgap_accession", "dbgap_accession"),

            // Additional fields for download
            Map.entry("study_id", "study_id")
        );

        return overview(PARTICIPANTS_END_POINT, params, PROPERTIES, defaultSort, mapping, "participants");
    }

    private List<Map<String, Object>> diagnosisOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Diagnoses
            new String[]{"diagnosis_pk", "diagnosis_pk"},
            new String[]{"age_at_diagnosis", "age_at_diagnosis_str"},
            new String[]{"anatomic_site", "anatomic_site"},
            new String[]{"diagnosis_basis", "diagnosis_basis"},
            new String[]{"diagnosis", "diagnosis"},
            new String[]{"diagnosis_classification_system", "diagnosis_classification_system"},
            new String[]{"disease_phase", "disease_phase"},
            new String[]{"tumor_classification", "tumor_classification"},

            // Demographics
            new String[]{"participant_pk", "participant_pk"},
            new String[]{"participant_id", "participant_id"},

            // Studies
            new String[]{"dbgap_accession", "dbgap_accession"},

            // Additional fields for download
            new String[]{"diagnosis_id", "diagnosis_id"},
            new String[]{"diagnosis_comment", "diagnosis_comment"},
            new String[]{"study_id", "study_id"},
            new String[]{"toronto_childhood_cancer_staging", "toronto_childhood_cancer_staging"},
            new String[]{"tumor_grade", "tumor_grade"},
            new String[]{"tumor_stage_clinical_m", "tumor_stage_clinical_m"},
            new String[]{"tumor_stage_clinical_n", "tumor_stage_clinical_n"},
            new String[]{"tumor_stage_clinical_t", "tumor_stage_clinical_t"},

            // Additional fields for Cohort manifest download
            new String[]{"race", "race_str"},
            new String[]{"sex_at_birth", "sex_at_birth"},
        };

        String defaultSort = "diagnosis_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Diagnoses
            Map.entry("diagnosis_pk", "diagnosis_pk"),
            Map.entry("age_at_diagnosis", "age_at_diagnosis"),
            Map.entry("anatomic_site", "anatomic_site"),
            Map.entry("diagnosis_basis", "diagnosis_basis"),
            Map.entry("diagnosis", "diagnosis"),
            Map.entry("diagnosis_classification_system", "diagnosis_classification_system"),
            Map.entry("disease_phase", "disease_phase"),
            Map.entry("tumor_classification", "tumor_classification"),

            // Demographics
            Map.entry("participant_pk", "participant_pk"),
            Map.entry("participant_id", "participant_id"),

            // Studies
            Map.entry("dbgap_accession", "dbgap_accession"),

            // Additional fields for download
            Map.entry("diagnosis_id", "diagnosis_id"),
            Map.entry("diagnosis_comment", "diagnosis_comment"),
            Map.entry("study_id", "study_id"),
            Map.entry("toronto_childhood_cancer_staging", "toronto_childhood_cancer_staging"),
            Map.entry("tumor_grade", "tumor_grade"),
            Map.entry("tumor_stage_clinical_m", "tumor_stage_clinical_m"),
            Map.entry("tumor_stage_clinical_n", "tumor_stage_clinical_n"),
            Map.entry("tumor_stage_clinical_t", "tumor_stage_clinical_t")
        );

        return overview(DIAGNOSES_END_POINT, params, PROPERTIES, defaultSort, mapping, "diagnoses");
    }

    private List<Map<String, Object>> studyOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Studies
            new String[]{"study_pk", "study_pk"},
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_name", "study_name"},

            // Additional fields for download
            new String[]{"consent", "consent"},
            new String[]{"consent_number", "consent_number_str"},
            new String[]{"external_url", "external_url"},
            new String[]{"study_description", "study_description"},
            new String[]{"study_id", "study_id"},
        };

        String defaultSort = "dbgap_accession"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Studies
            Map.entry("study_pk", "study_pk"),
            Map.entry("dbgap_accession", "dbgap_accession"),
            Map.entry("study_name", "study_name"),

            // Additional fields for download
            Map.entry("consent", "consent"),
            Map.entry("consent_number", "consent_number"),
            Map.entry("external_url", "external_url"),
            Map.entry("study_description", "study_description"),
            Map.entry("study_id", "study_id")
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
        final String[][] PROPERTIES = new String[][]{
            // Participants
            new String[]{"participant_pk", "participant_pk"},
            new String[]{"participant_id", "participant_id"},

            // Studies
            new String[]{"dbgap_accession", "dbgap_accession"},

            // Survivals
            new String[]{"survival_pk", "survival_pk"},
            new String[]{"age_at_last_known_survival_status", "age_at_last_known_survival_status_str"},
            new String[]{"cause_of_death", "cause_of_death"},
            new String[]{"first_event", "first_event"},
            new String[]{"last_known_survival_status", "last_known_survival_status"},

            // Additional fields for download
            new String[]{"age_at_event_free_survival_status", "age_at_event_free_survival_status_str"},
            new String[]{"event_free_survival_status", "event_free_survival_status"},
            new String[]{"study_id", "study_id"},
            new String[]{"survival_id", "survival_id"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Participants
            Map.entry("participant_pk", "participant_pk"),
            Map.entry("participant_id", "participant_id"),

            // Studies
            Map.entry("dbgap_accession", "dbgap_accession"),

            // Survivals
            Map.entry("survival_pk", "survival_pk"),
            Map.entry("age_at_last_known_survival_status", "age_at_last_known_survival_status"),
            Map.entry("cause_of_death", "cause_of_death"),
            Map.entry("first_event", "first_event"),
            Map.entry("last_known_survival_status", "last_known_survival_status"),

            // Additional fields for download
            Map.entry("age_at_event_free_survival_status", "age_at_event_free_survival_status"),
            Map.entry("event_free_survival_status", "event_free_survival_status"),
            Map.entry("study_id", "study_id"),
            Map.entry("survival_id", "survival_id")
        );

        return overview(SURVIVALS_END_POINT, params, PROPERTIES, defaultSort, mapping, "survivals");
    }

    private List<Map<String, Object>> treatmentOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Participants
            new String[]{"participant_pk", "participant_pk"},
            new String[]{"participant_id", "participant_id"},

            // Studies
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_id", "study_id"},

            // Treatments
            new String[]{"treatment_pk", "treatment_pk"},
            new String[]{"treatment_id", "treatment_id"},
            new String[]{"age_at_treatment_start", "age_at_treatment_start_str"},
            new String[]{"age_at_treatment_end", "age_at_treatment_end_str"},
            new String[]{"treatment_type", "treatment_type"},
            new String[]{"treatment_agent_str", "treatment_agent_str"},
            new String[]{"treatment_agent", "treatment_agent"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Participants
            Map.entry("participant_pk", "participant_pk"),
            Map.entry("participant_id", "participant_id"),

            // Studies
            Map.entry("dbgap_accession", "dbgap_accession"),
            Map.entry("study_id", "study_id"),

            // Treatments
            Map.entry("treatment_pk", "treatment_pk"),
            Map.entry("treatment_id", "treatment_id"),
            Map.entry("age_at_treatment_start", "age_at_treatment_start"),
            Map.entry("age_at_treatment_end", "age_at_treatment_end"),
            Map.entry("treatment_type", "treatment_type"),
            Map.entry("treatment_agent", "treatment_agent_str"),
            Map.entry("treatment_agent_str", "treatment_agent_str")
        );

        return overview(TREATMENTS_END_POINT, params, PROPERTIES, defaultSort, mapping, "treatments");
    }

    private List<Map<String, Object>> treatmentResponseOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Participants
            new String[]{"participant_pk", "participant_pk"},
            new String[]{"participant_id", "participant_id"},

            // Studies
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_id", "study_id"},

            // Treatment Responses
            new String[]{"treatment_response_pk", "treatment_response_pk"},
            new String[]{"treatment_response_id", "treatment_response_id"},
            new String[]{"response", "response"},
            new String[]{"age_at_response", "age_at_response_str"},
            new String[]{"response_category", "response_category"},
            new String[]{"response_system", "response_system"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            // Participants
            Map.entry("participant_pk", "participant_pk"),
            Map.entry("participant_id", "participant_id"),

            // Studies
            Map.entry("dbgap_accession", "dbgap_accession"),
            Map.entry("study_id", "study_id"),

            // Treatment Responses
            Map.entry("treatment_response_pk", "treatment_response_pk"),
            Map.entry("treatment_response_id", "treatment_response_id"),
            Map.entry("response", "response"),
            Map.entry("age_at_response", "age_at_response"),
            Map.entry("response_category", "response_category"),
            Map.entry("response_system", "response_system")
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
    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping, String overviewType) throws IOException {
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

        final String[][] PROPERTIES = new String[][]{
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_description", "study_description"},
            new String[]{"num_participants", "num_participants"},
            new String[]{"num_diseases", "num_diseases"},
            new String[]{"num_anatomic_sites", "num_anatomic_sites"},
            new String[]{"num_survivals", "num_survivals"}
        };

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("dbgap_accession", "dbgap_accession"),
            Map.entry("study_description", "study_description"),
            Map.entry("num_participants", "num_participants"),
            Map.entry("num_diseases", "num_diseases"),
            Map.entry("num_anatomic_sites", "num_anatomic_sites"),
            Map.entry("num_survivals", "num_survivals")
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
        String[][] properties = new String[][]{
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_name", "study_name"},
            new String[]{"num_participants", "num_participants"},
            new String[]{"num_diseases", "num_diseases"}
        };

        Map<String, Object> query = esService.buildListQuery();
        Request request = new Request("GET", STUDIES_END_POINT);
        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }

    private List<Map<String, Object>> findParticipantIdsInList(Map<String, Object> params) throws IOException {
        final String[][] properties = new String[][]{
                new String[]{"participant_id", "participant_id"},
                new String[]{"study_id", "study_id"}
        };

        Map<String, Object> query = esService.buildListQuery(params, Set.of(), false);
        Request request = new Request("GET",PARTICIPANTS_END_POINT);

        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }

    private Map<String, String> mapSortOrder(String order_by, String direction, String defaultSort, Map<String, String> mapping) {
        String sortDirection = direction;
        if (!sortDirection.equalsIgnoreCase("asc") && !sortDirection.equalsIgnoreCase("desc")) {
            sortDirection = "asc";
        }

        String sortOrder = defaultSort; // Default sort order
        if (mapping.containsKey(order_by)) {
            sortOrder = mapping.get(order_by);
        } else {
            logger.info("Order: \"" + order_by + "\" not recognized, use default order");
        }
        return Map.of(sortOrder, sortDirection);
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
                List<Integer> bounds = (List<Integer>) params.get(key);
                if (bounds.size() >= 2) {
                    Integer lower = bounds.get(0);
                    Integer higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    keys.add(key.concat(lower.toString()).concat(higher.toString()));
                }
            } else {
                List<String> valueSet = (List<String>) params.get(key);
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
