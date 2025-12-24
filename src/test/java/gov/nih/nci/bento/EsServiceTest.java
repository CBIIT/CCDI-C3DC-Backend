package gov.nih.nci.bento;

import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.utility.TypeChecker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith( SpringRunner.class )
@SpringBootTest(classes = {
    gov.nih.nci.bento.service.ESService.class,
    gov.nih.nci.bento.model.ConfigurationDAO.class
})
@TestPropertySource(properties = {
    "bento.api.version=1.0.0-TEST",
    "auth.enabled=false",
    "auth.endpoint=http://localhost:8080/auth",
    "neo4j.url=bolt://localhost:7687",
    "neo4j.user=neo4j",
    "neo4j.password=password",
    "graphql.schema=graphql/ccdi-portal.graphql",
    "graphql.es_schema=graphql/ccdi-portal-private-es.graphql",
    "graphql.public.schema=graphql/ccdi-portal-public.graphql",
    "graphql.public.es_schema=graphql/ccdi-portal-public-es.graphql",
    "allow_graphql_query=true",
    "allow_graphql_mutation=false",
    "redis.enable=false",
    "redis.use_cluster=false",
    "redis.host=localhost",
    "redis.port=6379",
    "redis.ttl=-1",
    "es.host=localhost",
    "es.port=443",
    "es.scheme=https",
    "es.filter.enabled=true",
    "es.sign.requests=false",
    "es.service_name=es",
    "es.region=us-east-1",
    "test.queries_file=placeholder"
})
public class EsServiceTest {
    @Autowired
    @Qualifier("ESService")
    private ESService esService;

    @Test
    public void testbuildListQuery() {
        Map<String, Object> params = Map.of(
                "param1", List.of("value1", "value2")
        );
        Map<String, Object> builtQuery = esService.buildListQuery(params, Set.of());
        Map<String, Object> bool = null;
        List<Map<String, Object>> filter = null;
        List<String> param1 = null;
        Map<String, Object> query = null;
        Object boolRaw = null;
        Object filterRaw = null;
        Object queryRaw = null;
        Object termsRaw = null;

        assertNotNull(builtQuery);

        queryRaw = builtQuery.get("query");

        if (TypeChecker.isOfType(queryRaw, new TypeToken<Map<String, Object>>() {})) {
            @SuppressWarnings("unchecked")
            Map<String, Object> castedQuery = (Map<String, Object>) queryRaw;
            query = castedQuery;
        }

        assertNotNull(query);

        boolRaw = query.get("bool");

        if (TypeChecker.isOfType(boolRaw, new TypeToken<Map<String, Object>>() {})) {
            @SuppressWarnings("unchecked")
            Map<String, Object> castedBool = (Map<String, Object>) query.get("bool");
            bool = castedBool;
        }

        assertNotNull(bool);

        filterRaw = bool.get("filter");

        if (TypeChecker.isOfType(filterRaw, new TypeToken<List<Map<String, Object>>>() {})) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> castedFilter = (List<Map<String, Object>>) bool.get("filter");
            filter = castedFilter;
        }

        assertNotNull(filter);
        assertEquals(1, filter.size());

        termsRaw = filter.get(0).get("terms");

        if (TypeChecker.isOfType(termsRaw, new TypeToken<Map<String, List<String>>>() {})) {
            @SuppressWarnings("unchecked")
            Map<String, List<String>> castedTerms = (Map<String, List<String>>) termsRaw;
            param1 = castedTerms.get("param1");
        }

        assertEquals(2, param1.size());
        assertEquals("value1", param1.get(0));
        assertEquals("value2", param1.get(1));
    }
}
