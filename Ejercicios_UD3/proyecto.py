from csv import DictReader
import mysql.connector
from time import time

timmerStart = time()

# CREAMOS LA CONEXIÓN A LA BASE DE DATOS, con autocommit, para que se inserten los datos directamente
mydb = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="password",
    database="olimpiadas_extra",
    autocommit=True
)
mycursor = mydb.cursor()

# BORRAMOS LAS TABLAS PARA UNA CORRECTA INSERCIÓN DE LOS DATOS
mycursor.execute("DELETE FROM Participacion")
mycursor.execute("DELETE FROM Evento")
mycursor.execute("DELETE FROM Olimpiada")
mycursor.execute("DELETE FROM Equipo")
mycursor.execute("DELETE FROM Deportista")
mycursor.execute("DELETE FROM Deporte")

# DECLARAR VARIABLES
# "ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal"
deportes = {} # {"Sport": ['ID']}
deportistas = {} # {"Name": ["ID", "Sex", "Height", "Weight"]}
equipos = {} # {"Team": ["ID", "NOC"]}
eventos = {} # {"Event": ["ID", "id_olimpiada", "id_deporte"]}
olimpiadas = {} # {"Games": ["ID", "Year", "City"]}
participaciones = {} # {"DeportistaEvento": ["id_equipo", "Age", "Medal"]}

idDeporteAI = 1
idEquipoAI = 1
idEventoAI = 1
idOlimpiadaAI = 1

idDeportista = 1
idDeporte = idDeporteAI
idEquipo = idEquipoAI
idEvento = idEventoAI
idOlimpiada = idOlimpiadaAI
idParticipacionAI = 1

# LEEMOS EL FICHERO CSV LINEA POR LINEA
with open('data/athlete_events.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        idDeportista = row['ID']
        if not row['Sport'] in deportes:
            deportes[row['Sport']] = [idDeporteAI, row['Sport']]
            idDeporte = idDeporteAI
            idDeporteAI += 1
        else:
            idDeporte = deportes[row['Sport']][0]

        if not idDeportista in deportistas:
            if row['Height'] == 'NA':
                height = None
            else:
                height = row['Height']
            if row['Weight'] == 'NA':
                weight = None
            else:
                weight = row['Weight']

            deportistas[idDeportista] = [idDeportista, row['Name'], row['Sex'], height, weight]

        if not row['Team'] in equipos:
            equipos[row['Team']] = [idEquipoAI, row['Team'], row['NOC']]
            idEquipo = idEquipoAI
            idEquipoAI += 1
        else:
            idEquipo = equipos[row['Team']][0]

        if not row['Games'] in olimpiadas:
            olimpiadas[row['Games']] = [idOlimpiadaAI, row['Games'], row['Year'], row['Season'], row['City']]
            idOlimpiada = idOlimpiadaAI
            idOlimpiadaAI += 1
        else:
            idOlimpiada = olimpiadas[row['Games']][0]

        if not row['Event'] in eventos:
            eventos[row['Event']] = [idEventoAI, row['Event'], idOlimpiada, idDeporte]
            idEvento = idEventoAI
            idEventoAI += 1
        else:
            idEvento = eventos[row['Event']][0]

        if idParticipacionAI not in participaciones:
            if row['Age'] == 'NA':
                edad = None
            else:
                edad = row['Age']
            if row['Medal'] == 'NA':
                medalla = None
            else:
                medalla = row['Medal']
            participaciones[idParticipacionAI] = [str(row['ID']), idEvento, idEquipo, edad, medalla]
            idParticipacionAI += 1

    print(len(participaciones))
    def chunks(lst):
        for i in range(0, len(lst), 100000):
            yield lst[i:i + 100000]

# INSERTAMOS LOS DEPORTES
    sql = "INSERT INTO Deporte (id_deporte, nombre) VALUES (%s, %s)"
    listaDeportes = deportes.values()
    mycursor.executemany(sql, listaDeportes)
    # for key in deportes:
    #     sql = "INSERT INTO Deporte (id_deporte, nombre) VALUES (%s, %s)"
    #     val = deportes[key]
    #     print(val)
    #     mycursor.execute(sql, val)

# INSERTAMOS DEPORTISTAS
    sql = "insert into Deportista (id_deportista, nombre, sexo, peso, altura) values (%s, %s, %s, %s, %s)"
    listaDeportistas = list(deportistas.values())
    mycursor.executemany(sql, listaDeportistas)
    # for key in deportistas:
    #      sql = "insert into Deportista (id_deportista, nombre, sexo, peso, altura) values (%s, %s, %s, %s, %s)"
    #      val = deportistas[key]
    #
    #      print(val)
    #      mycursor.execute(sql, val)

# INSERTAMOS EQUIPOS
    sql = "insert into Equipo (id_equipo, nombre, iniciales) values (%s, %s, %s)"
    listaEquipos = list(equipos.values())
    mycursor.executemany(sql, listaEquipos)
    # for key in equipos:
    #      sql = "insert into Equipo (id_equipo, nombre, iniciales) values (%s, %s, %s)"
    #      val = equipos[key]
    #      print(val)
    #      # mycursor.execute(sql, val)

# INSERTAMOS OLIMPIADAS
    sql = "insert into Olimpiada (id_olimpiada, nombre, anio, temporada, ciudad) values (%s, %s, %s, %s, %s)"
    listaOlimpiadas = list(olimpiadas.values())
    mycursor.executemany(sql, listaOlimpiadas)
    # for key in olimpiadas:
    #     sql = "insert into Olimpiada (nombre, anio, temporada, ciudad) values (%s, %s, %s, %s)"
    #     val = olimpiadas[key]
    #     print(val)
    #     # mycursor.execute(sql, val)

# INSERTAMOS EVENTOS
    sql = "insert into Evento (id_evento, nombre, id_olimpiada, id_deporte) values (%s, %s, %s, %s)"
    listaEventos = list(eventos.values())
    mycursor.executemany(sql, listaEventos)
    # for key in eventos:
    #     sql = "insert into Evento (nombre, id_olimpiada, id_deporte) values (%s, %s, %S)"
    #     val = eventos[key]
    #     print(val)
    #     mycursor.execute(sql, val)

# INSERTAMOS PARTICIPACIONES
#     sql = "insert into Participacion (id_deportista, id_evento, id_equipo, edad, medalla) values (%s, %s, %s, %s, %s)"
#     listaParticipaciones = list(participaciones.values())
#     mycursor.executemany(sql, listaParticipaciones)
#     # participacionesPartida = chunks(listaParticipaciones)
#     # for lista in participacionesPartida:
#     #     mycursor.executemany(sql, lista)
#     # mycursor.executemany(sql, listaOlimpiadas)
    for key in participaciones:
        sql = "insert into Participacion (id_deportista, id_evento, id_equipo, edad, medalla) values (%s, %s, %s, %s, %s)"
        val = participaciones[key]
        print(val)
        mycursor.execute(sql, val)

    mydb.close()

timmerEnd = time()
time = timmerEnd - timmerStart
print(" HA TARDADO "+str(time))
