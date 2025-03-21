#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal du pipeline ETL
"""

import os
import sys
import argparse
import time

from etl.extractors.csv_extractor import CSVExtractor
from etl.transformers.data_transformer import DataTransformer
from etl.transformers.schema_transformer import SchemaTransformer
from etl.loaders.csv_loader import CSVLoader
from etl.loaders.db_loader import DBLoader
from etl.utils.config import Config
from etl.pipeline.pipeline_executor import PipelineExecutor

def main():
    """Fonction principale du pipeline ETL"""
    
    # Analyse des arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Pipeline ETL pour les données de pandémie")
    parser.add_argument("--load-to-db", action="store_true", help="Charger les données dans la base de données")
    parser.add_argument("--config", type=str, default="config.json", help="Chemin vers le fichier de configuration")
    args = parser.parse_args()
    
    # Chargement de la configuration
    config_data = Config.load_config(args.config)
    
    # Définition des chemins
    input_dir = config_data.get("input_dir", "data")
    output_dir = config_data.get("output_dir", "processed")
    
    # Création du répertoire de sortie s'il n'existe pas
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Récupération des fichiers d'entrée
    input_files = [
        os.path.join(input_dir, f) for f in os.listdir(input_dir) 
        if f.endswith('.csv') and os.path.isfile(os.path.join(input_dir, f))
    ]
    
    if not input_files:
        print(f"Aucun fichier CSV trouvé dans le répertoire {input_dir}")
        return
    
    print(f"Fichiers d'entrée: {', '.join(os.path.basename(f) for f in input_files)}")
    
    # Initialisation des composants du pipeline
    extractor = CSVExtractor()
    transformer = DataTransformer()
    schema_transformer = SchemaTransformer()
    csv_loader = CSVLoader()
    
    # Initialisation du chargeur de base de données si nécessaire
    db_loader = None
    if args.load_to_db:
        db_config = config_data.get("database", {})
        if not db_config:
            print("Configuration de la base de données manquante")
            return
        db_loader = DBLoader(db_config)
    
    # Initialisation de l'exécuteur du pipeline
    pipeline = PipelineExecutor(
        extractor, 
        transformer, 
        schema_transformer, 
        csv_loader, 
        db_loader
    )
    
    # Exécution du pipeline
    start_time = time.time()
    results = pipeline.run(input_files, output_dir, args.load_to_db)
    end_time = time.time()
    
    # Affichage des résultats
    print("\n=== RÉSULTATS DU PIPELINE ETL ===")
    print(f"Temps d'exécution: {end_time - start_time:.2f} secondes")
    print(f"Lignes extraites: {results['extraction']}")
    print(f"Lignes transformées: {results['transformation']}")
    
    print("\nTables préparées:")
    for table, count in results['schema'].items():
        print(f"  {table}: {count} lignes")
    
    print("\nFichiers CSV générés:")
    for file, count in results['csv_loading'].items():
        print(f"  {file}: {count} lignes")
    
    if args.load_to_db:
        print("\nDonnées chargées dans la base de données:")
        for table, count in results['db_loading'].items():
            print(f"  {table}: {count} lignes")
    
    print("\nPipeline ETL terminé avec succès!")

if __name__ == "__main__":
    main()
