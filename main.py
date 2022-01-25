from datetime import datetime
import sqlite3
import requests as rs

URL_API = 'http://api.pioupiou.fr/v1/live/'
DEBUG = False


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def connect_database(db_name):
    conn = sqlite3.connect(db_name)
    return conn


def create_database(db_name, dump=False):
    conn = connect_database(db_name + '.db')
    c = conn.cursor()
    if dump:
        c.execute('DROP TABLE stations')
    c.execute('''CREATE TABLE IF NOT EXISTS stations(
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                lat DOUBLE,
                lng DOUBLE,
                wind_speed_avg DOUBLE,
                date_update DATE NOT NULL,
                id_station INTEGER NOT NULL
                );
                ''')
    conn.commit()
    conn.close()


def create_database_backup(db_name_backup, db_name_origin):
    conn = connect_database(db_name_origin + '.db')
    c = conn.cursor()
    c.execute('SELECT * FROM stations')
    data = c.fetchall()
    conn.close()
    
    create_database(db_name_backup, dump=True)
    conn = connect_database(db_name_backup + '.db')
    c = conn.cursor()

    if DEBUG:
        print(data)
    
    c.executemany('INSERT INTO stations(id, name, lat, lng, wind_speed_avg, date_update, id_station) VALUES (?,?,?,?,?,?,?)', data)
    conn.commit()
    conn.close()

def clear_database(db_name, nb_value=10):
    print(bcolors.WARNING + "Suppression des données de la table stations (Nombres de lignes supprimé : "+ str(nb_value) +")" + bcolors.ENDC)
    conn = connect_database(db_name + '.db')
    c = conn.cursor()
    for i in range(nb_value):
        c.execute('DELETE FROM stations WHERE id = (SELECT MAX(id) from stations)')
        conn.commit()   
    conn.close()


def count_value_in_db(db_name):
    conn = connect_database(db_name + '.db')
    c = conn.cursor()
    nb_value = c.execute('SELECT COUNT(*) FROM stations').fetchone()
    conn.close()
    return nb_value[0]


def get_station_byId(url, station_id):
    response = rs.get(url + station_id)
    return response.json()['data']


def add_station_in_db(db_name, station):
    conn = connect_database(db_name + '.db')
    c = conn.cursor()
    isExist = c.execute(
        'SELECT COUNT(*), date_update FROM stations WHERE id_station = {0} ORDER BY date_update DESC'.format(station['id'])).fetchone()
    if isExist[1] is not None:
        date_db = isExist[1].split(" ")[0]
    else:
        date_db = None

    if DEBUG:
        print(date_db, datetime.now().strftime("%Y-%m-%d"))

    if isExist[0] == 0 or date_db != datetime.now().strftime("%Y-%m-%d"):

        sql = 'INSERT INTO stations(name, lat, lng, wind_speed_avg, date_update, id_station) VALUES ("{0}", {1}, {2}, {3}, "{4}", {5})'.format(
            station['meta']['name'], station['location']['latitude'], station['location']['longitude'], station['measurements']['wind_speed_avg'], datetime.now(), station['id'])
        c.execute(sql)
        conn.commit()
    conn.close()

def run_script():
    id_station = input("Quelle station voulez vous ajouter ? (id)\n")
    station_data = get_station_byId(URL_API, id_station)
    create_database('weather_station')

    check_nb_value = count_value_in_db('weather_station')
    if check_nb_value >= 10:
        clear_database('weather_station', 3 + (check_nb_value - 10))

    add_station_in_db('weather_station', station_data)
    check_nb_value = count_value_in_db('weather_station')
    if check_nb_value % 5 == 0:
        print(bcolors.OKGREEN + "Création d'une sauvegarde" + bcolors.ENDC)
        create_database_backup('weather_station_backup', 'weather_station')


def main():
    user_input = input(
        "Quel action faire : (!run : lancer le script | !drop : supprimer toute les données de la table stations) | !backup : créer une base de données de secours\n")
    if user_input == "!run":
        run_script()
    elif user_input == "!drop":
        clear_database("weather_station")
        print(bcolors.WARNING + "Toutes les données de la table stations ont été supprimées" + bcolors.ENDC)
    elif user_input == "!backup":
        create_database_backup('weather_station_backup', 'weather_station')
        print(bcolors.OKGREEN + "Création d'une sauvegarde" + bcolors.ENDC)
    else:
        print("Commande inconnue")


main()
