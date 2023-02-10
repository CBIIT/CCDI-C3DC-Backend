package gov.nih.nci.bento.graphql;

import gov.nih.nci.bento.model.AbstractESDataFetcher;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.ConfigurationDAO;
import graphql.GraphQL;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphql.SchemaBuilder;
import org.neo4j.graphql.SchemaConfig;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

@Component
public class BentoGraphQL {

    private static final Logger logger = LogManager.getLogger(BentoGraphQL.class);

    private final GraphQL privateGraphQL;

    public BentoGraphQL(
            ConfigurationDAO config,
            AbstractPrivateESDataFetcher privateESDataFetcher
    ) throws IOException {
        this.privateGraphQL = buildGraphQLWithES(config.getEsSchemaFile(), privateESDataFetcher);
    }

    public GraphQL getPrivateGraphQL() {
        return privateGraphQL;
    }

    private GraphQL buildGraphQLWithES(String esSchemaFile, AbstractESDataFetcher esBentoDataFetcher) throws IOException {
        GraphQLSchema esSchema = getEsSchema(esSchemaFile, esBentoDataFetcher);
        return GraphQL.newGraphQL(esSchema).build();
    }
    
    private GraphQLSchema getEsSchema(String esSchema, AbstractESDataFetcher bentoDataFetcher) throws IOException {
        File schemaFile = new DefaultResourceLoader().getResource("classpath:" + esSchema).getFile();
        TypeDefinitionRegistry schemaParser = new SchemaParser().parse(schemaFile);
        return new SchemaGenerator().makeExecutableSchema(schemaParser, bentoDataFetcher.buildRuntimeWiring());
    }
}
