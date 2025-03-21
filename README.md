
## Prérequis

- Python 3.7 ou supérieur
- Bibliothèques Python : pandas, numpy, mysql-connector-python


## Installation

1. Clone
2. Install Python :

```bash
pip install pandas numpy mysql-connector-python
```

3. les fichiers CSV sources doivent être présents dans le dir racine.

## Utilisation

### Exécution du processus ETL complet

Pour exécuter le processus ETL complet

```bash
python etl_process.py
```

### Test du processus ETL

Pour tester les différentes étapes du processus ETL séparément :

```bash
python test_etl.py
```

### Création de la BDD

1. Connect to MySQL/MariaDB
2. Exécute le script SQL pour créer la base de données et les tables :

```bash
mysql -u <utilisateur> -p < epiviz.sql
```

3. Pour charger les données, modifiez la configuration de la bdd dans `etl_process.py` .

### 3. Chargement

Les données transformées sont :
1. Sauvegardées dans des fichiers CSV intermédiaires dans le dossier `processed/`
2. Prêtes à être chargées dans la base de données MySQL/MariaDB

## Prérequis

- Python 3.8 ou supérieur
- MySQL Server 5.7 ou supérieur
- Bibliothèques Python requises (voir ci-dessous)

### Installation des dépendances

```bash
pip install pandas mysql-connector-python
```

## Configuration

1. Crée une base de données MySQL en utilisant le script `epiviz.sql` :

```bash
mysql -u root -p < epiviz.sql
```

2.  paramètres de connexion dans le fichier `config.json` :

```json
{
    "input_dir": ".",
    "output_dir": "processed",
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "",
        "database": "epiviz",
        "batch_size": 1000
    }
}
```

## Utilisation

### Exécution du pipeline ETL sans chargement dans la base de données

```bash
python etl_pipeline.py
```#   c l e a n d a t a v 1  
 