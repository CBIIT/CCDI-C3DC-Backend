# Known Gaps

## Missing Traces
- **Unknown**: Application bootstrap class (for example `@SpringBootApplication` main class) is not present in `src/main/java`.
- **Unknown**: HTTP transport wiring to GraphQL execution (servlet/controller registration path) is not visible in current source.
- **Unknown**: Internals of inherited base classes from `gov.nih.nci.bento.*` used by RI classes.

## Doc/Code Mismatches
- **Observed mismatch**: `README.md` describes a broader Bento backend with Neo4j-centric setup guidance; current source snapshot primarily shows RI GraphQL/OpenSearch/CPI extensions.
- **Observed mismatch**: `CPI_FETCHER_README.md` references `gov.nih.nci.bento.controller.CPIController` and several `gov.nih.nci.bento.*` file paths that are not present in current source tree.
- **Observed mismatch**: `src/main/resources/application_local.*` points to `bento-extended*.graphql` files, while repository GraphQL files are named `ccdi-portal-*.graphql`.

## Unclear Ownership
- **Unknown**: Whether `gov.nih.nci.bento.*` classes are maintained in another repository/module, generated, or omitted from this checkout.
- **Unknown**: Ownership boundary between this RI layer (`bento_ri`) and upstream Bento framework code.

## Unverified Assumptions
- **Inferred**: WAR deployment is intended for Tomcat (`Dockerfile` + web.xml), but complete runtime bootstrap chain is not directly observable.
- **Inferred**: Redis settings may be legacy/compatibility configuration in this repo snapshot; active RI code path uses Caffeine cache.

## Recommended Next Investigations
1. Locate the source of `gov.nih.nci.bento.*` classes used by imports and tests.
2. Trace how GraphQL schema files are selected at runtime (especially local `bento-extended*` vs committed `ccdi-portal-*`).
3. Confirm whether any REST controller path is expected for CPI, or if CPI is GraphQL-only in current architecture.
4. Add a focused module doc for `PrivateESDataFetcher` when resolver-level change work is requested.
