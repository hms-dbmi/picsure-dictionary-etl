## Code Formatting
Before contributing code, please set up our git hook: `cp code-formatting/pre-commit.sh .git/hooks/pre-commit`

    To skip formatting on a block of code, wrap in spotless:off, spotless:on comments

## Uploading a TSV file for `/anvil/upload-tsv` data ingest
```aiignore
curl -X POST -H "Content-Type: text/plain" --data-binary @path/to/your/file.tsv http://localhost:8080/anvil/upload-tsv
```

## Local Database for Development

To set up a local development database, follow these steps:

1. Run the `docker-compose.postgres.yml` file to start the required services:
   ```bash
   docker-compose -f docker-compose.postgres.yml up --build -d
   ```

2. Execute the `db/schema.sql` script to create the `dict` schema and all necessary database tables.

3. *(Optional)* If you have a dump file generated with `pg_dump`, you can restore it using `pg_restore`. If the backup is from our `BDC` database, the following command will work:
   ```bash
   pg_restore \
      --host="localhost" \
      --port="5432" \
      --username="username" \
      --dbname="dictionary_db" \
      --jobs=10 \
      --verbose \
      --no-owner \
      ./dictionary_db
   ```
   You will be prompted to enter the database password after executing this command.

4. Now you can run the Spring Boot application with `dev` as the active profile.

### Installing `pg_restore` and `pg_dump` on Mac

If you do not have `pg_restore` and `pg_dump`, you can install them by following these steps:

1. Install `postgresql@16`, which includes both `pg_restore` and `pg_dump`:
   ```bash
   brew install postgresql@16
   ```

2. Link the installed PostgreSQL version:
   ```bash
   brew link postgresql@16
   ```

3. Add PostgreSQL to your `zshrc` or `bash` profile. For `zshrc`, use the following command:
   ```bash
   echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
   ```

4. Verify the versions of `pg_dump` and `pg_restore`:
   ```bash
   pg_dump --version
   pg_restore --version
   ```

   If the command doesn’t work immediately, you may need to restart your terminal.


## Facet Loader

This project includes a Facet Loader that ingests a JSON payload describing Facet Categories, their Facets (including nested children), and mapping rules to automatically associate facets with concept nodes based on a concept path.

What it does:
- Upserts facet categories and facets by name (idempotent). If a name already exists it is updated; otherwise it is created.
- Builds a facet hierarchy using parent_id based on the JSON nesting of facets.
- Evaluates expressions to map facets to concept nodes (rows in dict.concept_node) and writes relationships into dict.facet__concept_node.

API endpoint:
- POST /api/facet-loader/load
- Request body is a JSON array of objects each containing a Facet_Category.
- Returns a JSON object with counts: categoriesCreated, categoriesUpdated, facetsCreated, facetsUpdated.

Example request body (minimal):
[
  {
    "Facet_Category": {
      "Name": "Consortium_Curated_Facets",
      "Display": "Consortium Curated Facets",
      "Description": "Consortium Curated Facets Description",
      "Facets": [
        {
          "Name": "Recover Adult",
          "Display": "RECOVER Adult",
          "Description": "Recover adult parent facet.",
          "Facets": [
            {
              "Name": "Infected",
              "Display": "Infected",
              "Description": "Infected Facet Description"
            }
          ]
        }
      ]
    }
  }
]

Example with expressions (maps facets to concept nodes by concept_path):
- Concept path sample: \\phs003436\\Recover_Adult\\biostats_derived\\visits\\inf\\12\\pasc_cc_2024\\
- Nodes (by index):
  - 0: phs003436
  - 1: Recover_Adult
  - 2: biostats_derived
  - 3: visits
  - 4: inf
  - 5: 12
  - 6: pasc_cc_2024
- Negative indices are allowed (e.g., -1 is last node, -2 second to last, etc.).

Example payload fragment using expressions:
{
  "Facet_Category": {
    "Name": "Consortium_Curated_Facets",
    "Facets": [
      {
        "Name": "Recover Adult",
        "Expressions": [
          { "Logic": "equal", "Regex": "(?i)Recover_Adult$", "Node_Position": 1 }
        ],
        "Facets": [
          {
            "Name": "Infected",
            "Expressions": [
              { "Logic": "equal", "Regex": "(?i)\\binf(ected)?\\b", "Node_Position": -3 }
            ]
          },
          {
            "Name": "Adult Dataset (not derived)",
            "Expression": [
              { "Logic": "not", "Regex": "(?i).*derived.*", "Node_Position": 2 }
            ]
          }
        ]
      }
    ]
  }
}

Expression evaluation rules:
- Supported Logic values: equal and not (case-insensitive).
- Regex is Java regex; inline flags such as (?i) for case-insensitive are supported.
- nodePosition is zero-based, negative values allowed to index from the end (e.g., -1 is last node).
- All expressions listed for a facet are ANDed; all must match for a concept_path to qualify.
- Out-of-bounds node positions or invalid regex cause the expression to evaluate to false.
- If a facet contains no expressions, it is not automatically mapped to any concept nodes (still created in the hierarchy).

Alias handling in payload:
- Facet name must be provided as Name.
- Expressions can be provided as Expressions or Expression.
- Node position must be provided as Node_Position.

Pre-requisites for mapping:
- dict.concept_node should already be populated (e.g., via the Dictionary Loader) so that concept_path values exist to evaluate.

cURL example:
- Save payload to payload.json and run:
  curl -X POST \
       -H "Content-Type: application/json" \
       --data-binary @payload.json \
       http://localhost:8080/api/facet-loader/load

Sample response:
{
  "categoriesCreated": 1,
  "categoriesUpdated": 0,
  "facetsCreated": 5,
  "facetsUpdated": 0
}

Idempotency and updates:
- Re-posting the same payload will update existing categories/facets and keep the hierarchy; counts will reflect updates rather than creations.
- Facet-to-concept mappings avoid duplicates using ON CONFLICT semantics in the repository/service layer.

Tests you can run (Docker required for Testcontainers):
- mvn -Dtest=edu.harvard.dbmi.avillach.dictionaryetl.facetloader.FacetLoaderControllerTest test
- mvn -Dtest=edu.harvard.dbmi.avillach.dictionaryetl.facetloader.FacetLoaderServiceTest test
- mvn -Dtest=edu.harvard.dbmi.avillach.dictionaryetl.facetloader.FacetLoaderMappingIntegrationTest test
- mvn -Dtest=edu.harvard.dbmi.avillach.dictionaryetl.facetloader.FacetExpressionEvaluatorTest test
Or run all tests:
- mvn test

Troubleshooting:
- If no concept nodes are mapped after posting, verify that concept_node rows exist and your node positions/regex align with your concept_path structure.
- Ensure the app is running on the expected port and CORS origin (Facet Loader controller is configured with CrossOrigin for http://localhost:8081 by default).