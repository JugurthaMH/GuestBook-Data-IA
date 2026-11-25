# Apprentissages

## Extract → Bronze

On réalise un versionning de dump de BDD sans modifier les données brutes lors de l'export.

### Problème rencontré et pourquoi il est survenu
Une petite étourderie, j'ai oublié de dire que les données de customers seront exportés sur la table bronze.customers.

### Solution appliquée et pourquoi cette solution fonctionne
J'ai corrigé le fichier extract.py et j'ai relancé le script.

### Ce que j'ai appris
J'ai appris le concept du Bronze, donc le versionning de dump de BDD.

```
uv run python pipelines/extract.py
```

```
uv run python -c "import duckdb; print(duckdb.connect('warehouse.duckdb').sql('SELECT count(*) FROM bronze.orders'))"
```

```
uv run python -c "import duckdb; print(duckdb.connect('warehouse.duckdb').sql('SELECT count(*) FROM bronze.customers'))"
```
