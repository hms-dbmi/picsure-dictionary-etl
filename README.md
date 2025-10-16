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


## Table of Contents

- [Code Formatting](#code-formatting)
- [Uploading a TSV file for /anvil/upload-tsv data ingest](#uploading-a-tsv-file-for-anvilupload-tsv-data-ingest)
- [Local Database for Development](#local-database-for-development)
- [Facet Loader](#facet-loader)
  - [API: Load Facets](#facet-loader)
  - [Expression Evaluation](#expression-evaluation-rules)
  - [Clear facets and categories](#clear-facets-and-categories)
  - [Testing with FacetLoader.http](#testing-requests-with-facetloaderhttp)
- [Tests](#tests-you-can-run-docker-required-for-testcontainers)
- [Troubleshooting](#troubleshooting)

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
          { "exactly": "Recover_Adult", "node": 1 }
        ],
        "Facets": [
          {
            "Name": "Infected",
            "Expressions": [
              { "regex": "(?i)\\\binf(ected)?\\\b", "node": -3 }
            ]
          }
        ]
      }
    ]
  }
}

Expression evaluation rules:
- Supported keys per expression entry: exactly, contains, regex (use one or more).
- Each entry must include node (zero-based index; negatives allowed, e.g., -1 is last node).
- Semantics: all expression entries are ANDed. Within a single entry, all provided keys must match the node value.
- Regex uses Java syntax; inline flags like (?i) are supported. Exactly/contains are literal string matches.
- Out-of-bounds node indices or invalid regex cause that entry to evaluate to false.
- If a facet contains no expressions, it is not automatically mapped to any concept nodes (still created in the hierarchy).

Alias handling in payload:
- Facet name must be provided as Name.
- Expressions can be provided as Expressions or Expression.
- Node index must be provided as node.

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

## Clear facets and categories

Use this endpoint to remove facet categories and/or specific facets (including all their descendants) by name. The service will also remove facet-to-concept mappings first to preserve referential integrity.

- Endpoint: POST /api/facet-loader/clear
- Request body (JSON):

  {
    "facetCategories": ["CategoryName1", "CategoryName2"],
    "facets": ["FacetNameA", "FacetNameB"]
  }

- Both properties are optional. Provide one or both:
  - facetCategories: Deletes each listed category by name, all facets within, and all mappings. Returns counts.
  - facets: Deletes each listed facet by name and all of its descendants in the hierarchy, plus all related mappings. Other categories/facets remain intact.

- Response body (JSON):

  {
    "categoriesDeleted": 1,
    "facetsDeleted": 7,
    "mappingsDeleted": 42
  }

Behavior notes:
- Deletions are name-based and case-sensitive to match stored names.
- For categories, the service deletes mappings, then facets, then the category record.
- For facets, the service deletes mappings, then deletes the facet and all descendants in a breadth-first traversal.
- Requests are idempotent: nonexistent names are ignored (counted as zero deletions).

cURL example (clear by category):

  curl -X POST \
       -H "Content-Type: application/json" \
       --data-binary '{
         "facetCategories": ["Consortium_Curated_Facets"]
       }' \
       http://localhost:8080/api/facet-loader/clear

cURL example (clear by facet names):

  curl -X POST \
       -H "Content-Type: application/json" \
       --data-binary '{
         "facets": ["Recover Adult", "Infected"]
       }' \
       http://localhost:8080/api/facet-loader/clear

## Testing requests with FacetLoader.http

A FacetLoader.http file is included at the repository root with ready-to-run sample requests compatible with the IntelliJ HTTP Client.

How to use:
- Open FacetLoader.http in IntelliJ IDEA or any IDE that supports the HTTP Client.
- Adjust the @host variable at the top if your application runs on a different port (e.g., 8086 in Docker).
- Click the gutter icons to execute requests for loading facets and clearing by category/facet names.
