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
