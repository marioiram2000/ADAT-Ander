from csv import DictReader
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="password",
    database="olimpiadas_extra",
    autocommit=True
)
mycursor = mydb.cursor()

# "ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal"
deportes = {} # {"Sport": ['ID']}
deportistas = {} # {"Name": ["ID", "Sex", "Height", "Weight"]}
equipos = {} # {"Team": ["ID", "NOC"]}
eventos = {} # {"Event": ["ID", "id_olimpiada", "id_deporte"]}
olimpiadas = {} # {"Games": ["ID", "Year", "City"]}
participaciones = {} # {"DeportistaEvento": ["id_equipo", "Age", "Medal"]}

idDeporte = 1
idEquipo = 1
idEvento = 1
idOlimpiada = 1
idParticipacion = 1

with open('data/athlete_events.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        if not row['Sport'] in deportes:
            deportes[row['Sport']] = [idDeporte, row['Sport']]
            idDeporte += 1

        if not row['Name'] in deportistas:
            deportistas[row['Name']] = [row['ID'], row['Name'], row['Sex'], row['Height'], row['Weight']]

        if not row['Team'] in equipos:
            equipos[row['Team']] = [idEquipo, row['NOC']]
            idEquipo += 1

        if not row['Event'] in eventos:
            eventos[row['Event']] = [idEvento, idOlimpiada, idDeporte]
            idEvento += 1

        if not row['Games'] in olimpiadas:
            olimpiadas[row['Games']] = [idOlimpiada, row['Year'], row['City']]
            idOlimpiada += 1

        idParticipacion = str(row['ID'])+"_"+str(idEquipo)
        if not row[idParticipacion] in participaciones:
            participaciones[idParticipacion] = [str(row['ID']), idEvento, row['Age'], row['Medal']]

    print(deportes, deportistas, equipos, eventos, olimpiadas, participaciones)
    for key in deportes:
        sql = "INSERT INTO Deporte (id_deporte, nombre) VALUES (%s, %s)"
        val = deportes[key]
        # mycursor.execute(sql, val)

    mydb.close()
