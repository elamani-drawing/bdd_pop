from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment
from config import params

# Configurer la connection dans config.py
connection = get_connection() 
initialise_db(connection, params, flag_reset_bdd=True)

# Importe seulment les statistiques de populations des lignes de 5 à 20 premieres  
insert_stats_population_fragment(connection, params,annee="2020", init_metadonnee = True,  first=5, last = 20, verbose=True)  
# De 21 à 30
insert_stats_population_fragment(connection, params,annee="2020", first=21, last = 30, verbose=True) 
# Vous pouvez passer last à null pour tout importer  (De 31 à toute la fin du fichier)
insert_stats_population_fragment(connection, params,annee="2020", first=31, verbose=True) 
# Importe seulement les 21 premieres lignes de tout les fichiers des statistiques sur les mariages (DEP 1 à 6)
insert_dep_file_fragment(connection, params, "2021", last = 20, verbose=True) 
# Importe toutes les statistiques de mariage (de 21 à la fin de chaque fichier)
insert_dep_file_fragment(connection, params,"2021", first=21, last = None, verbose=True)  

connection.close()