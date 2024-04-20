from lib import initialise_db, login_bdd, insert_stats_population_fragment, insert_dep_file_fragment, execute_view_bdd, execute_alter_procedure
from config import params
from requete import get_population_departments_after_alter, get_population_regions_after_alter

connection = login_bdd("bddproject_db", "root", "root", "localhost", verbose=True)

def restore_data(connection, params):
    initialise_db(connection, params, flag_reset_bdd=True)
    insert_stats_population_fragment(connection, params, annee="2020",init_metadonnee= True, verbose=True)  
    insert_dep_file_fragment(connection, params,"2021", verbose=True)  
    execute_view_bdd(connection, params) # genere les view du fichier view.sql
    execute_alter_procedure(connection, params) # execute le fichier alter.sql et procedure.sql, comme indiquer a la question 3
    
def print_get_pop_dep_after_alter(connection):
    departements = get_population_departments_after_alter(connection)
    print("\nPopulation des départements:")
    for row in departements:
        print(f"- Département {row[0]} ({row[1]}) avec une population de {row[2]} habitants")

def print_get_pop_reg_after_alter(connection):
    regions = get_population_regions_after_alter(connection) 
    print("\nPopulation des régions:")
    for row in regions:
        print(f"- Région {row[0]} ({row[1]}) avec une population de {row[2]} habitants")
        
# Vous pouvez commenter cette ligne apres qu'elle est etait executé au moin une fois 
restore_data(connection, params) 
print_get_pop_dep_after_alter(connection)
print_get_pop_reg_after_alter(connection)

connection.close()