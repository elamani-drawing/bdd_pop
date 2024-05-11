from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment, execute_view_bdd, execute_alter_procedure, execute_sql_file
from config import params
from requete import insert_region

# Configurer la connection dans config.py
connection = get_connection() 

def restore_data(connection, params):
    initialise_db(connection, params, flag_reset_bdd=True)
    insert_stats_population_fragment(connection, params, annee="2020", init_metadonnee= True, verbose=True)
    insert_dep_file_fragment(connection, params,"2021", verbose=True)
    execute_view_bdd(connection, params) # genere les view du fichier view.sql
    # execute le fichier alter.sql et procedure.sql, comme indiquer a la question 3
    # execute le fichier trigger.sql pour empecher insert/update/ delete de departement et region
    # calcul et met a jour les calculs de population à l'ajout d'une nouvelle année de recensement quand le resultat est pertinent
    execute_alter_procedure(connection, params) 
    execute_sql_file(connection, params["sql_requete"]["trigger_sql"]) 
    
        
# Vous pouvez commenter cette ligne apres qu'elle est etait executé au moin une fois 
restore_data(connection, params) 
print("- Une erreur est attendu:")
# L'insertion est censé echoué à cause des triggers qui empeche l'insertion sur Région et Département
insert_region(connection, 999, "Région qui n'existe pas.") 

connection.close()