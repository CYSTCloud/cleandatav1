# Architecture du Pipeline ETL

Ce document explique l'architecture modulaire du pipeline ETL et le rôle de chaque fichier dans le système.

## Vue d'ensemble

Le pipeline ETL (Extract, Transform, Load) a été refactorisé en suivant les principes de séparation des responsabilités. Cette architecture modulaire améliore la lisibilité, la maintenabilité, la testabilité et la réutilisabilité du code.

## Structure des répertoires

```
ETLCSV1/
├── etl/                      # Package principal
│   ├── extractors/           # Modules d'extraction
│   ├── transformers/         # Modules de transformation
│   ├── loaders/              # Modules de chargement
│   ├── utils/                # Utilitaires
│   └── pipeline/             # Orchestration du pipeline
├── data/                     # Données d'entrée
├── processed/                # Données transformées
├── etl_pipeline.py           # Script principal
└── config.json               # Configuration
```

## Rôle de chaque fichier

### Script principal

- **etl_pipeline.py** : Point d'entrée du pipeline ETL. Ce script analyse les arguments de la ligne de commande, initialise les composants du pipeline et lance l'exécution.

### Extracteurs

- **etl/extractors/csv_extractor.py** : Responsable de l'extraction des données à partir des fichiers CSV. Contient des méthodes pour lire différents types de fichiers CSV.

### Transformateurs

- **etl/transformers/data_transformer.py** : Classe principale qui coordonne la transformation des données brutes. Utilise les transformateurs spécifiques pour chaque type de données.
- **etl/transformers/covid_transformer.py** : Transforme les données COVID-19 provenant de différentes sources.
- **etl/transformers/monkeypox_transformer.py** : Transforme les données de la variole du singe (Monkeypox).
- **etl/transformers/schema_transformer.py** : Prépare les données selon le schéma SQL de la base de données. Coordonne la préparation des tables de référence et de la table de données principale.
- **etl/transformers/reference_tables.py** : Contient les classes pour préparer les tables de référence (calendrier, localisation, pandemie).
- **etl/transformers/data_table.py** : Responsable de la préparation de la table de données principale qui contient les cas, décès, etc.

### Chargeurs

- **etl/loaders/csv_loader.py** : Sauvegarde les DataFrames transformés dans des fichiers CSV.
- **etl/loaders/db_loader.py** : Classe principale pour le chargement des données dans une base de données MySQL. Coordonne le processus de chargement.
- **etl/loaders/db_connection.py** : Gère la connexion à la base de données MySQL, avec des méthodes pour établir/fermer la connexion et vérifier la structure des tables.
- **etl/loaders/table_loaders.py** : Contient des classes spécifiques pour charger chaque type de table (calendrier, localisation, pandemie, data).

### Utilitaires

- **etl/utils/config.py** : Gère la configuration du pipeline, avec des méthodes pour charger et sauvegarder les paramètres.

### Pipeline

- **etl/pipeline/pipeline_executor.py** : Orchestre l'exécution du pipeline ETL en coordonnant les différentes étapes (extraction, transformation, chargement).

## Flux de données

1. **Extraction** : Les fichiers CSV sont lus par `CSVExtractor`.
2. **Transformation** : Les données brutes sont transformées par `DataTransformer` qui utilise les transformateurs spécifiques.
3. **Préparation du schéma** : Les données transformées sont préparées selon le schéma SQL par `SchemaTransformer`.
4. **Chargement CSV** : Les données préparées sont sauvegardées dans des fichiers CSV par `CSVLoader`.
5. **Chargement DB** (optionnel) : Les données sont chargées dans une base de données MySQL par `DBLoader`.

## Avantages de cette architecture

- **Lisibilité** : Chaque module a une responsabilité unique et clairement définie.
- **Maintenabilité** : Les modifications peuvent être apportées à un composant sans affecter les autres.
- **Testabilité** : Chaque composant peut être testé indépendamment.
- **Réutilisabilité** : Les composants peuvent être réutilisés dans d'autres projets.

## Utilisation

Le pipeline peut être exécuté de deux façons :
- `python etl_pipeline.py` : Exécute le pipeline ETL sans chargement dans la base de données.
- `python etl_pipeline.py --load-to-db` : Exécute le pipeline ETL avec chargement dans la base de données.

Un fichier de configuration `config.json` peut être spécifié avec l'option `--config`.
