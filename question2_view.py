from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment, execute_view_bdd
from config import params
from requete import view_get_population_departments, view_get_population_regions

# Configurer la connection dans config.py
connection = get_connection() 

def restore_data(connection, params):
    initialise_db(connection, params, flag_reset_bdd=True)
    insert_stats_population_fragment(connection, params, annee="2020",init_metadonnee= True, verbose=True)  
    insert_dep_file_fragment(connection, params,"2021", verbose=True)  
    execute_view_bdd(connection, params) # genere les view du fichier view.sql
    
def print_view_get_pop_dep(connection):
    departements = view_get_population_departments(connection)
    print("\nPopulation des départements par année:")
    for row in departements:
        print(f"{row[2]} - Département {row[0]} ({row[1]}) : {row[3]} habitants")

def print_view_get_pop_reg(connection):
    regions = view_get_population_regions(connection) 
    print("\nPopulation des régions par année:")
    for row in regions:
        print(f"{row[2]} - Région {row[0]} ({row[1]}) : {row[3]} habitants")
        
# Vous pouvez commenter cette ligne apres qu'elle est etait executé au moin une fois 
restore_data(connection, params) 

print_view_get_pop_dep(connection)
print_view_get_pop_reg(connection)

connection.close()