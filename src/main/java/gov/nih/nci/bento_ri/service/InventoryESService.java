package gov.nih.nci.bento_ri.service;

import com.google.gson.*;
import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.service.RedisService;
import gov.nih.nci.bento.service.connector.AWSClient;
import gov.nih.nci.bento.service.connector.AbstractClient;
import gov.nih.nci.bento.service.connector.DefaultClient;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.*;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

@Service("InventoryESService")
public class InventoryESService extends ESService {
    public static final String SCROLL_ENDPOINT = "/_search/scroll";
    public static final String JSON_OBJECT = "jsonObject";
    public static final String AGGS = "aggs";
    public static final int MAX_ES_SIZE = 500000;
    final Set<String> PARTICIPANT_PARAMS = Set.of("ethnicity", "race", "sex_at_birth");
    final Set<String> DIAGNOSIS_PARAMS = Set.of(
        "age_at_diagnosis", "anatomic_site", "diagnosis_classification",
        "diagnosis_classification_system", "diagnosis_verification_status",
        "diagnosis_basis", "disease_phase"
    );
    final Set<String> STUDY_PARAMS = Set.of(
        "phs_accession", "study_acronym", "study_short_title"
    );
    final Set<String> SAMPLE_PARAMS = Set.of("sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification");
    final Set<String> SURVIVAL_PARAMS = Set.of("age_at_last_known_survival_status", "first_event", "last_known_survival_status");
    final Set<String> FILE_PARAMS = Set.of("assay_method", "file_type", "library_selection", "library_source", "library_strategy");
    final Set<String> SAMPLE_FILE_PARAMS = Set.of("sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "assay_method", "file_type", "library_selection", "library_source", "library_strategy");
    

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

    @PreDestroy
    private void close() throws IOException {
        client.close();
    }

    public JsonObject send(Request request) throws IOException{
        Response response = client.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            String msg = "Elasticsearch returned code: " + statusCode;
            logger.error(msg);
            throw new IOException(msg);
        }
        return getJSonFromResponse(response);
    }

    public JsonObject getJSonFromResponse(Response response) throws IOException {
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
        return jsonObject;
    }

    // This function build queries with following rules:
    //  - If a list is empty, query will return empty dataset
    //  - If a list has only one element which is empty string, query will return all data available
    //  - If a list is null, query will return all data available
    public Map<String, Object> buildListQuery(Map<String, Object> params, Set<String> excludedParams) {
        return buildListQuery(params, excludedParams, false);
    }

    public Map<String, Object> buildListQuery(Map<String, Object> params, Set<String> excludedParams, boolean ignoreCase) {
        Map<String, Object> result = new HashMap<>();

        List<Object> filter = new ArrayList<>();
        for (var key: params.keySet()) {
            if (excludedParams.contains(key)) {
                continue;
            }
            Object obj = params.get(key);

            List<String> valueSet;
            if (obj instanceof List) {
                valueSet = (List<String>) obj;
            } else {
                String value = (String)obj;
                valueSet = List.of(value);
            }

            if (ignoreCase) {
                List<String> lowerCaseValueSet = new ArrayList<>();
                for (String value: valueSet) {
                    lowerCaseValueSet.add(value.toLowerCase());
                }
                valueSet = lowerCaseValueSet;
            }
            // list with only one empty string [""] means return all records
            if (valueSet.size() == 1) {
                if (valueSet.get(0).equals("")) {
                    continue;
                }
            }
            filter.add(Map.of(
                "terms", Map.of( key, valueSet)
            ));
        }

        result.put("query", Map.of("bool", Map.of("filter", filter)));
        return result;
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams, Set<String> excludedParams, Set<String> regular_fields, String nestedProperty, String indexType) throws IOException {
        Map<String, Object> result = new HashMap<>();

        List<Object> filter = new ArrayList<>();
        List<Object> participant_filters = new ArrayList<>();
        List<Object> diagnosis_filters = new ArrayList<>();
        List<Object> sample_filters = new ArrayList<>();
        List<Object> survival_filters = new ArrayList<>();
        List<Object> file_filters = new ArrayList<>();
        List<Object> sample_file_filters = new ArrayList<>();
        
        for (String key: params.keySet()) {
            String finalKey = key;
            if (indexType.equals("files") && key.equals("assay_method")) {
                    finalKey = "file_category";
            }
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
                    } else if (!indexType.equals("samples") && key.equals("participant_age_at_collection")) {
                        sample_file_filters.add(Map.of(
                            "range", Map.of(nestedProperty+"."+key, range)
                        ));
                    } else if (!indexType.equals("survivals") && key.equals("age_at_last_known_survival_status")) {
                        survival_filters.add(Map.of(
                            "range", Map.of("survivals." + key, range)
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
                
                if (indexType.equals("files")) {
                    if (key.equals("assay_method")) {
                        key = "file_category";
                    }
                    if (key.equals("participant_ids")) {
                        key = "file_participant_ids";
                    }
                }
                if (key.equals("participant_ids")) {
                    key = "participant_id";
                }
                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    if (PARTICIPANT_PARAMS.contains(key) && indexType.equals("files")) {
                        participant_filters.add(Map.of(
                            "terms", Map.of("participant_filters."+key, valueSet)
                        ));
                    } else if (DIAGNOSIS_PARAMS.contains(key) && !indexType.equals("diagnoses")) {
                        diagnosis_filters.add(Map.of(
                            "terms", Map.of("diagnoses." + key, valueSet)
                        ));
                    } else if (SAMPLE_FILE_PARAMS.contains(key) && !(indexType.equals("samples") || indexType.equals("files"))) {
                        sample_file_filters.add(Map.of(
                            "terms", Map.of("sample_file_filters."+key, valueSet)
                        ));
                    } else if (SAMPLE_PARAMS.contains(key) && indexType.equals("files")) {
                        sample_filters.add(Map.of(
                            "terms", Map.of("sample_filters."+key, valueSet)
                        ));
                    } else if (SURVIVAL_PARAMS.contains(key) && !indexType.equals("survivals")) {
                        survival_filters.add(Map.of(
                            "terms", Map.of("survivals." + key, valueSet)
                        ));
                    } else if (FILE_PARAMS.contains(key) && indexType.equals("samples")) {
                        file_filters.add(Map.of(
                            "terms", Map.of("file_filters."+key, valueSet)
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
        int sampleFileFilterLen = sample_file_filters.size();
        int sampleFilterLen = sample_filters.size();
        int survivalFilterLen = survival_filters.size();
        int fileFilterLen = file_filters.size();
        if (FilterLen + participantFilterLen + diagnosisFilterLen + sampleFileFilterLen + sampleFilterLen + survivalFilterLen + fileFilterLen == 0) {
            result.put("query", Map.of("match_all", Map.of()));
        } else {
            if (participantFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "participant_filters", "query", Map.of("bool", Map.of("filter", participant_filters)), "inner_hits", Map.of())));
            }
            if (diagnosisFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "diagnoses", "query", Map.of("bool", Map.of("filter", diagnosis_filters)), "inner_hits", Map.of())));
            }
            if (sampleFileFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "sample_file_filters", "query", Map.of("bool", Map.of("filter", sample_file_filters)), "inner_hits", Map.of())));
            }
            if (sampleFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "sample_filters", "query", Map.of("bool", Map.of("filter", sample_filters)), "inner_hits", Map.of())));
            }
            if (survivalFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "survivals", "query", Map.of("bool", Map.of("filter", survival_filters)), "inner_hits", Map.of())));
            }
            if (fileFilterLen > 0) {
                filter.add(Map.of("nested", Map.of("path", "file_filters", "query", Map.of("bool", Map.of("filter", file_filters)), "inner_hits", Map.of())));
            }
            result.put("query", Map.of("bool", Map.of("filter", filter)));
        }
        
        return result;
    }

    public Map<String, Object> buildGetFileIDsQuery(List<String> ids) throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.put("_source", Set.of("id", "files"));
        result.put("query", Map.of("terms", Map.of("id", ids)));
        result.put("size", ids.size());
        result.put("from", 0);
        return result;
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
        fields.put(nodeName, Map.of("terms", Map.of("field", nodeName)));
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addRangeCountAggregations(Map<String, Object> query, String rangeAggName, String cardinalityAggName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);

            //   "aggs": {
            //     "age_at_diagnosis": {
            //        "range": {
            //           "field": "age_at_diagnosis",
            //           "ranges": [
            //              {
            //                 "from": 0,
            //                 "to": 1000
            //              },
            //              {
            //                 "from": 1000,
            //                 "to": 10000
            //              },
            //              {
            //                 "from": 10000,
            //                 "to": 25000
            //              },
            //              {
            //                 "from": 25000
            //              }
            //           ]
            //        },
            //        "aggs": {
            //           "unique_count": {
            //            "cardinality": {
            //                "field": "participant_id",
            //                "precision_threshold": 40000
            //            }
            //          }
            //        }
            //      }
            //    }

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
        newQuery.put("size", 0);

        //       "aggs": {
        //         "inner": {
        //           "filter":{  
        //             "range":{  
        //              "age_at_diagnosis":{  
        //               "gt":0
        //              }
        //             }
        //            },
        //            "aggs": {
        //              "age_stats": { 
        //               "stats": { 
        //                 "field": "age_at_diagnosis"
        //               }
        //             }
        //           }

        Map<String, Object> fields = new HashMap<String, Object>();
        Map<String, Object> subField = new HashMap<String, Object>();
        subField.put("filter", Map.of("range", Map.of(rangeAggName, Map.of("gt", -1))));
        subField.put("aggs", Map.of("range_stats", Map.of("stats", Map.of("field", rangeAggName))));
        fields.put("inner", subField);
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String subCardinalityAggName, String[] rangeAggNames, List<String> only_includes) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);
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
        newQuery.put("aggs", fields);
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

    public List<String> collectFileIDs(JsonObject jsonObject) {
        List<String> data = new ArrayList<>();
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        for (var hit: searchHits) {
            JsonObject obj = hit.getAsJsonObject().get("_source").getAsJsonObject();
            JsonArray arr = obj.get("files").getAsJsonArray();
            for (int i = 0; i < arr.size(); i++) {
                data.add(arr.get(i).getAsString());
            }
        }
        return data;
    }

    public List<String> collectTerms(JsonObject jsonObject, String aggName) {
        List<String> data = new ArrayList<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        JsonArray buckets = aggs.getAsJsonObject(aggName).getAsJsonArray("buckets");
        for (var bucket: buckets) {
            data.add(bucket.getAsJsonObject().get("key").getAsString());
        }
        return data;
    }

    public Map<String, JsonObject> collectRangeAggs(JsonObject jsonObject, String[] rangeAggNames) {
        Map<String, JsonObject> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        for (String aggName: rangeAggNames) {
            // Range/stats
            data.put(aggName, aggs.getAsJsonObject(aggName));
        }
        return data;
    }

    public List<Map<String, Object>> collectPage(Request request, Map<String, Object> query, String[][] properties, int pageSize, int offset) throws IOException {
        // data over limit of Elasticsearch, have to use roll API
        if (pageSize > MAX_ES_SIZE) {
            throw new IOException("Parameter 'first' must not exceeded " + MAX_ES_SIZE);
        }
        if (pageSize + offset > MAX_ES_SIZE) {
            return collectPageWithScroll(request, query, properties, pageSize, offset);
        }

        // data within limit can use just from/size
        query.put("size", pageSize);
        query.put("from", offset);
        // System.out.println(gson.toJson(query));
        request.setJsonEntity(gson.toJson(query));

        JsonObject jsonObject = send(request);
        return collectPage(jsonObject, properties, pageSize);
    }

    // offset MUST be multiple of pageSize, otherwise the page won't be complete
    private List<Map<String, Object>> collectPageWithScroll(
            Request request, Map<String, Object> query, String[][] properties, int pageSize, int offset) throws IOException {
        final int optimumSize = ( MAX_ES_SIZE / pageSize ) * pageSize;
        if (offset % pageSize != 0) {
            throw new IOException("'offset' must be multiple of 'first'!");
        }
        query.put("size", optimumSize);
        request.setJsonEntity(gson.toJson(query));
        request.addParameter("scroll", "10S");
        JsonObject page = rollToPage(request, offset);
        return collectPage(page, properties, pageSize, offset % optimumSize);
    }

    private JsonObject rollToPage(Request request, int offset) throws IOException {
        int rolledRecords = 0;
        JsonObject jsonObject = send(request);
        String scrollId = jsonObject.get("_scroll_id").getAsString();
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        rolledRecords += searchHits.size();

        while (rolledRecords <= offset && searchHits.size() > 0) {
            // Keep roll until correct page
            logger.info("Current records: " + rolledRecords + " collecting...");
            Request scrollRequest = new Request("POST", SCROLL_ENDPOINT);
            Map<String, Object> scrollQuery = Map.of(
                    "scroll", "10S",
                    "scroll_id", scrollId
            );
            scrollRequest.setJsonEntity(gson.toJson(scrollQuery));
            jsonObject = send(scrollRequest);
            scrollId = jsonObject.get("_scroll_id").getAsString();
            searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
            rolledRecords += searchHits.size();
        }

        // Now return page
        scrollId = jsonObject.get("_scroll_id").getAsString();
        Request clearScrollRequest = new Request("DELETE", SCROLL_ENDPOINT);
        clearScrollRequest.setJsonEntity("{\"scroll_id\":\"" + scrollId +"\"}");
        send(clearScrollRequest);
        return jsonObject;
    }

    // Collect a page of data, result will be of pageSize or less if not enough data remains
    public List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, int pageSize) throws IOException {
        return collectPage(jsonObject, properties, pageSize, 0);
    }

    private List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, int pageSize, int offset) throws IOException {
        return collectPage(jsonObject, properties, null, pageSize, offset);
    }

    public List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, String[][] highlights, int pageSize, int offset) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();

        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        for (int i = 0; i < searchHits.size(); i++) {
            // skip offset number of documents
            if (i + 1 <= offset) {
                continue;
            }
            Map<String, Object> row = new HashMap<>();
            for (String[] prop: properties) {
                String propName = prop[0];
                String dataField = prop[1];
                JsonElement element = searchHits.get(i).getAsJsonObject().get("_source").getAsJsonObject().get(dataField);
                row.put(propName, getValue(element));
            }
            data.add(row);
            if (data.size() >= pageSize) {
                break;
            }
        }
        return data;
    }

    // Convert JsonElement into Java collections and primitives
    private Object getValue(JsonElement element) {
        Object value = null;
        if (element == null || element.isJsonNull()) {
            return null;
        } else if (element.isJsonObject()) {
            value = new HashMap<String, Object>();
            JsonObject object = element.getAsJsonObject();
            for (String key: object.keySet()) {
                ((Map<String, Object>) value).put(key, getValue(object.get(key)));
            }
        } else if (element.isJsonArray()) {
            value = new ArrayList<>();
            for (JsonElement entry: element.getAsJsonArray()) {
                ((List<Object>)value).add(getValue(entry));
            }
        } else {
            value = element.getAsString();
        }
        return value;
    }

}