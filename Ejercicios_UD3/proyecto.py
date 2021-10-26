from csv import DictReader
import mysql.connector
from time import time
# MIRAMOS EL MOMENTO EN EL QUE EMPIEZA LA EJECUCIÓN, PARA LUEGO VER LO QUE TARDA
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
# LINEA: "ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal"
deportes = {} # {"Sport": [idDeporteAI, row['Sport']}
deportistas = {} # {idDeportista: ["ID", "Sex", "Height", "Weight"]}
equipos = {} # {"Team": [idEquipoAI, 'Team', 'NOC']}
olimpiadas = {} # {"Games": [idOlimpiadaAI, 'Games', 'Year', 'Season', 'City']}
eventos = {} # {"Event_idOlimpiada": [idEventoAI, row['Event'], idOlimpiada, idDeporte]}
participaciones = {} # {idParticipacionAI: [idDeportista, idEvento, idEquipo, edad, medalla]}

# GENERO CLAVES QUE IRÉ INCREMENTANDO PARA LOS VALORES QUE NO TIENEN ID
idDeporteAI = 1
idEquipoAI = 1
idEventoAI = 1
idOlimpiadaAI = 1
idParticipacionAI = 1

# NECESITO OTRA VARIABLE PARA ALMACENAR EL ID DE LOS CAMPOS CORRESPONDIENTES DE LA LINEA EN LA QUE ESTÉ
idDeportista = 1
idDeporte = idDeporteAI
idEquipo = idEquipoAI
idEvento = idEventoAI
idOlimpiada = idOlimpiadaAI

# LEEMOS EL FICHERO CSV LINEA POR LINEA
with open('data/athlete_events.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        idDeportista = row['ID']  # EL ID DE LA LINEA ES EL DEL DEPORTISTA QUE HACE ESA PARTICIPACIÓN

        # SI NO TENGO EL DEPORTE EN LA LISTA, LO AÑADO, SI LO TENGO, COJO EL ID DE ESE DEPORTE
        if row['Sport'] not in deportes:
            deportes[row['Sport']] = [idDeporteAI, row['Sport']]
            idDeporte = idDeporteAI
            idDeporteAI += 1
        else:
            idDeporte = deportes[row['Sport']][0]

        # SI NO TENGO EL DEPORTISTA EN LA LISTA, LO AÑADO. COMPRUEBO LOS VALORES QUE PUEDEN SER 'NA'
        if idDeportista not in deportistas:
            if row['Height'] == 'NA':
                height = None
            else:
                height = row['Height']
            if row['Weight'] == 'NA':
                weight = None
            else:
                weight = row['Weight']

            deportistas[idDeportista] = [idDeportista, row['Name'], row['Sex'], height, weight]

        # SI NO TENGO EL EQUPIO EN LA LISTA, LO AÑADO, SI LO TENGO, COJO EL ID DE ESE EQUIPO
        if row['Team'] not in equipos:
            equipos[row['Team']] = [idEquipoAI, row['Team'], row['NOC']]
            idEquipo = idEquipoAI
            idEquipoAI += 1
        else:
            idEquipo = equipos[row['Team']][0]

        # SI NO TENGO LA OLIMPIADA EN LA LISTA, LA AÑADO, SI LA TENGO, COJO EL ID DE ESA OLIMPIADA
        if row['Games'] not in olimpiadas:
            olimpiadas[row['Games']] = [idOlimpiadaAI, row['Games'], row['Year'], row['Season'], row['City']]
            idOlimpiada = idOlimpiadaAI
            idOlimpiadaAI += 1
        else:
            idOlimpiada = olimpiadas[row['Games']][0]

        # ENLAZO EL NOMBRE DEL EVENTO CON LA OLIMPIADA PARA GENERAR LA KEY DEL DICCIONARIO, LOS EVENTOS PUEDEN TENER
        # EL MISMO NOMBRE (P.E Basketball Men's Basketball) PARA DIFERENTES OLIMPIADAS.
        # SI NO LO TENGO, LO AÑADO, SI LO TENGO, COJO EL ID.
        # PARA LOS EVENTOS SE NECESITAN LOS IDS QUE HEMOS IDO RECOGIENDO
        if row['Event']+"_"+str(idOlimpiada) not in eventos:
            eventos[row['Event']+"_"+str(idOlimpiada)] = [idEventoAI, row['Event'], idOlimpiada, idDeporte]
            idEvento = idEventoAI
            idEventoAI += 1
        else:
            idEvento = eventos[row['Event']+"_"+str(idOlimpiada)][0]

        # CADA LINEA DEL ARCHIVO ES UNA PARTICIPACIÓN, NO NECESITO COMPROBAR SI LO TENGO O NO. COMPRUEBO LOS CAMPOS 'NA'
        # PARA LAS PARTICIPACIONES TAMBIEN SE NECESITAN LOS IDS QUE HEMOS IDO RECOGIENDO
        if idParticipacionAI not in participaciones:
            if row['Age'] == 'NA':
                edad = None
            else:
                edad = row['Age']
            if row['Medal'] == 'NA':
                medalla = None
            else:
                medalla = row['Medal']
            participaciones[idParticipacionAI] = [idDeportista, idEvento, idEquipo, edad, medalla]

        idParticipacionAI += 1  # CLAVE DE PARTICIPACIÓN Y CONTADOR DE LA LINEA

    # FUNCIÓN PARA PARTIR UNA LISTA (NO SE USA)
    def chunks(lst):
        for i in range(0, len(lst), 100000):
            yield lst[i:i + 100000]

# PARA LAS INSERCIONES HAY DOS MODOS, CON EL EXECUTEMANY, QUE LE PASAMOS UNA LISTA Y UNA A UNA RECORRIENDO LOS
# DICCIONARIOS. PARA OBTENER UNA LISTA DESDE LOS VALORES DE UN DICCIONARIO, USAMOS LA FUNCION VALUES() Y LO CASTEAMOS
# A LIST, YA QUE NO ES UNA LISTA COMO TAL.

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
    sql = "insert into Participacion (id_deportista, id_evento, id_equipo, edad, medalla) values (%s, %s, %s, %s, %s)"
    listaParticipaciones = list(participaciones.values())
    mycursor.executemany(sql, listaParticipaciones)
#     # participacionesPartida = chunks(listaParticipaciones)
#     # for lista in participacionesPartida:
#     #     mycursor.executemany(sql, lista)
#     # mycursor.executemany(sql, listaOlimpiadas)
#     for key in participaciones:
#         sql = "insert into Participacion (id_deportista, id_evento, id_equipo, edad, medalla)" \
#               "values (%s, %s, %s, %s, %s)"
#         val = participaciones[key]
#         print(val)
#         mycursor.execute(sql, val)

    mydb.close()

# MIRO EL MOMENTO EN EL QUE TERMINA LA EJECUCIÓN Y HAGO LA RESTA DE TIEMPOS PARA VER LO QUE HA TARDADO
timmerEnd = time()
time = timmerEnd - timmerStart
print("HA TARDADO "+str(time))
