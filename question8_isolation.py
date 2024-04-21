import psycopg2.pool
from threading import Thread, get_ident
from config import connexion, params
from lib import initialise_db, get_connection, insert_stats_population_fragment, insert_dep_file_fragment, execute_view_bdd, execute_alter_procedure


def restore_data(connection, params):
    initialise_db(connection, params, flag_reset_bdd=True)
    insert_stats_population_fragment(connection, params, annee="2020",init_metadonnee= True, verbose=True)  
    insert_dep_file_fragment(connection, params,"2021", verbose=True)  
    execute_view_bdd(connection, params) # genere les view du fichier view.sql
    execute_alter_procedure(connection, params) # execute le fichier alter.sql et procedure.sql, comme indiquer a la question 3
    
    
connection_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, dbname=connexion["dbname"], user=connexion["username"], password=connexion["password"], host=connexion["host"], port=connexion["port"])


def update_value(value):
    """
    Met à jour la valeur dans la table
    """
    
    connection = connection_pool.getconn()

    cursor = connection.cursor()
    cursor.execute("UPDATE datasstatsmariage SET value = '%s' WHERE id_dep = '08' and id_statistique_mariage = '2151'", (value,))
    connection.commit()
    cursor.close()

    connection_pool.putconn(connection)


def read_value():
    """
    Lit la valeur actuelle dans la table
    """
    connection = connection_pool.getconn()

    cursor = connection.cursor()
    cursor.execute("SELECT value FROM datasstatsmariage WHERE id_dep = '08' and id_statistique_mariage = '2151'")
    result = cursor.fetchone()
    cursor.close()

    connection_pool.putconn(connection)

    return result[0] if result else None


def run_transaction(isolation_level):
    """
    Exécute une transaction avec un niveau d'isolation spécifié
    """
    try:
        connection = connection_pool.getconn()
        connection.set_session(isolation_level=isolation_level)
        thread_id = get_ident()

        # Affiche l'identifiant du thread et le niveau d'isolation
        print(f"Thread {thread_id}: Isolation level: {isolation_level}")

        # Lit la valeur initiale
        initial_value = read_value()

        # Met à jour la valeur
        update_value(thread_id)
        
        print(f"Thread {thread_id}: Lit la valeur initiale: {initial_value}, Remplace la valeur par: {thread_id}")


        # Lit à nouveau la valeur
        updated_value = read_value()
        print(f"Thread {thread_id}: Relit la valeur apres mis à jour: {updated_value}")

    except Exception as e:
        print(f"Thread {thread_id}: Error:", e)

    finally:

        connection_pool.putconn(connection)

def test_isolation_levels():
    """
    Teste différentes niveaux d'isolation en parallèle
    """
    isolation_levels = [
        psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
        psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
        psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
        psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    ]

    # Crée un thread pour chaque niveau d'isolation
    threads = []
    for isolation_level in isolation_levels:
        thread = Thread(target=run_transaction, args=(isolation_level,))
        threads.append(thread)
        thread.start()

    # Attend que tous les threads se terminent
    for thread in threads:
        thread.join()
 
# Vous pouvez commenter cette ligne apres qu'elle est etait executé au moin une fois 
restore_data(get_connection(), params) 
# Exécute le test
test_isolation_levels()
print("\nVoir la fin du fichier python pour avoir des explications.")

"""
Thread 64412: Isolation level: 4
Thread 67780: Isolation level: 1
Thread 67904: Isolation level: 2
Thread 68116: Isolation level: 3
Thread 64412: Lit la valeur initiale: 40_49, Remplace la valeur par: 64412
Thread 67780: Lit la valeur initiale: 64412, Remplace la valeur par: 67780
Thread 64412: Relit la valeur apres mis à jour: 67780
Thread 68116: Lit la valeur initiale: 64412, Remplace la valeur par: 68116
Thread 67904: Lit la valeur initiale: 64412, Remplace la valeur par: 67904
Thread 68116: Relit la valeur apres mis à jour: 68116
Thread 67780: Relit la valeur apres mis à jour: 68116
Thread 67904: Relit la valeur apres mis à jour: 68116

Dans cet exemple, nous avons lancé quatre threads qui effectuent des opérations de lecture et d'écriture sur la même ligne dans la base de données. 
- Thread 64412 (ISOLATION_LEVEL_SERIALIZABLE) :
    Lit la valeur initiale de la base de données, qui est 40_49.
    Remplace la valeur par l'identifiant du thread, donc la nouvelle valeur est 64412.
    Relit la valeur après la mise à jour et obtient 67780. Cependant, cela est dû au fait que le thread 64412 a été interrompu par 
    le thread 67780 avant de pouvoir lire la valeur mise à jour par lui-même.

-Thread 67780 (ISOLATION_LEVEL_READ_UNCOMMITTED) :
    Lit la valeur initiale de la base de données, qui est 64412.
    Remplace la valeur par l'identifiant du thread, donc la nouvelle valeur est 67780.
    Le thread 67780 lit la valeur mise à jour immédiatement après l'avoir écrite, car il utilise le niveau d'isolation READ UNCOMMITTED, 
    qui permet de voir les modifications non validées par d'autres transactions.

- Thread 68116 (ISOLATION_LEVEL_REPEATABLE_READ) :
    Lit la valeur initiale de la base de données, qui est 64412.
    Remplace la valeur par l'identifiant du thread, donc la nouvelle valeur est 68116.
    Le thread 68116 lit la valeur initiale qu'il a lue avant d'effectuer sa propre mise à jour, car il utilise le niveau d'isolation REPEATABLE READ, 
    qui garantit une lecture cohérente des données pendant la transaction.

    - Thread 67904 (ISOLATION_LEVEL_READ_COMMITTED) :
    Lit la valeur initiale de la base de données, qui est 64412.
    Remplace la valeur par l'identifiant du thread, donc la nouvelle valeur est 67904.
    Le thread 67904 lit la valeur initiale qu'il a lue avant d'effectuer sa propre mise à jour, car il utilise le niveau d'isolation READ COMMITTED, 
    qui garantit la lecture de données validées par d'autres transactions.
"""