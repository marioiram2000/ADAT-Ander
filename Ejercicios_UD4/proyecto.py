from csv import DictReader
import mysql.connector
import sqlite3

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Query, Session, sessionmaker

Base = declarative_base()


def getMySQLConnection():
    return create_engine("mysql+pymysql://admin:password@localhost/olimpiadas_extra", echo=False)


def getSQLiteConnection():
    return create_engine('sqlite:///sqlite.db', echo=False)


class Deporte(Base):
    __tablename__ = 'Deporte'
    id_deporte = Column(Integer, primary_key=True)
    nombre = Column(String)


class Deportista(Base):
    __tablename__ = 'Deportista'
    id_deportista = Column(Integer, primary_key=True)
    nombre = Column(String)
    sexo = Column(String)
    peso = Column(Integer)
    altura = Column(Integer)


class Equipo(Base):
    __tablename__ = 'Equipo'
    id_equipo = Column(Integer, primary_key=True)
    nombre = Column(String)
    iniciales = Column(String)


class Olimpiada(Base):
    __tablename__ = 'Olimpiada'
    id_olimpiada = Column(Integer, primary_key=True)
    nombre = Column(String)
    anio = Column(Integer)
    temporada = Column(String)
    ciudad = Column(String)


class Evento(Base):
    __tablename__ = 'Evento'
    id_evento = Column(Integer, primary_key=True)
    nombre = Column(String)
    id_olimpiada = Column(Integer, ForeignKey('Olimpiada.id_olimpiada'))
    id_deporte = Column(Integer, ForeignKey('Deporte.id_deporte'))
    olimpiada = relationship("Olimpiada", back_populates="eventos")


Olimpiada.eventos = relationship("Evento", back_populates="olimpiada")


class Participacion(Base):
    __tablename__ = 'Participacion'
    id_deportista = Column(Integer, ForeignKey('Deportista.id_deportista'), primary_key=True)
    id_evento = Column(Integer, ForeignKey('Evento.id_evento'), primary_key=True)
    id_equipo = Column(Integer, ForeignKey('Equipo.id_equipo'))
    edad = Column(Integer)
    medalla = Column(String)

    deportista = relationship("Deportista", back_populates="participaciones")
    evento = relationship("Evento", back_populates="participaciones")
    equipo = relationship("Equipo", back_populates="participaciones")

    __table_args__ = (
        PrimaryKeyConstraint(id_deportista, id_evento),
        {},
    )


Deportista.participaciones = relationship("Participacion", back_populates="deportista")
Evento.participaciones = relationship("Participacion", back_populates="evento")
Equipo.participaciones = relationship("Participacion", back_populates="equipo")


# FUNCIÓN PARA PARTIR UNA LISTA (NO SE USA)
def chunks(lst):
    for i in range(0, len(lst), 100000):
        yield lst[i:i + 100000]


# FUNCIÓN PARA BORRAR LAS TABLAS
def borrarTablas(session):
    session.execute("DELETE FROM Participacion;")
    session.execute("DELETE FROM Evento;")
    session.execute("DELETE FROM Olimpiada;")
    session.execute("DELETE FROM Equipo;")
    session.execute("DELETE FROM Deportista;")
    session.execute("DELETE FROM Deporte;")


# FUNCIÓN PARA CREAR LAS TABLAS
def crearTablas(session):
    session.execute("CREATE TABLE Deporte (id_deporte, nombre);")
    session.execute("CREATE TABLE Deportista (id_deportista, nombre, sexo, peso, altura);")
    session.execute("CREATE TABLE Equipo (id_equipo, nombre, iniciales);")
    session.execute("CREATE TABLE Evento (id_evento, nombre, id_olimpiada, id_deporte);")
    session.execute("CREATE TABLE Olimpiada (id_olimpiada, nombre, anio, temporada, ciudad);")
    session.execute("CREATE TABLE Participacion (id_deportista, id_evento, id_equipo, edad, medalla);")


# FUNCIÓN PARA SELECCIONAR LA BD
def seleccionarBD():
    bbdd = input("¿Que base de datos desea usar? (MySQL/SQLite)")
    while (bbdd.lower() != "mysql") and (bbdd.lower() != "sqlite"):
        bbdd = input("Introduzca una base de datos correcta (MySQL/SQLite)")
    if bbdd.lower() == "mysql":
        db = getMySQLConnection()
    else:
        db = getSQLiteConnection()
    return db


# FUNCIÓN DE INSERCIÓN DE DATOS
def insertarDatos(session):
    # BORRAMOS LAS TABLAS PARA UNA CORRECTA INSERCIÓN DE LOS DATOS
    try:
        borrarTablas(session)
    except sqlite3.OperationalError:
        crearTablas(session)

    # DECLARAR VARIABLES
    # LINEA: "ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year",
    # "Season","City","Sport","Event","Medal"
    deportes = {}  # {"Sport": [idDeporteAI, row['Sport']}
    deportistas = {}  # {idDeportista: ["ID", "Sex", "Height", "Weight"]}
    equipos = {}  # {"Team": [idEquipoAI, 'Team', 'NOC']}
    olimpiadas = {}  # {"Games": [idOlimpiadaAI, 'Games', 'Year', 'Season', 'City']}
    eventos = {}  # {"Event_idOlimpiada": [idEventoAI, row['Event'], idOlimpiada, idDeporte]}
    participaciones = {}  # {idParticipacionAI: [idDeportista, idEvento, idEquipo, edad, medalla]}

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
    with open('../Ejercicios_UD3/data/athlete_events.csv', 'r') as read_obj:
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
            if row['Event'] + "_" + str(idOlimpiada) not in eventos:
                eventos[row['Event'] + "_" + str(idOlimpiada)] = [idEventoAI, row['Event'], idOlimpiada, idDeporte]
                idEvento = idEventoAI
                idEventoAI += 1
            else:
                idEvento = eventos[row['Event'] + "_" + str(idOlimpiada)][0]

            # CADA LINEA DEL ARCHIVO ES UNA PARTICIPACIÓN, NO NECESITO COMPROBAR SI LO TENGO O NO.
            # COMPRUEBO LOS CAMPOS 'NA'
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

        # INSERTAMOS LOS DEPORTES
        for key in deportes:
            row = deportes[key]
            d = Deporte(id_deporte=row[0], nombre=row[1])
            session.add(d)

        # INSERTAMOS DEPORTISTAS
        for key in deportistas:
            row = deportistas[key]
            d = Deportista(id_deportista=row[0], nombre=row[1], sexo=row[2], peso=row[3], altura=row[4])
            session.add(d)

        # INSERTAMOS EQUIPOS
        for key in equipos:
            row = equipos[key]
            e = Equipo(id_equipo=row[0], nombre=row[1], iniciales=row[2])
            session.add(e)

        # INSERTAMOS OLIMPIADAS
        for key in olimpiadas:
            row = olimpiadas[key]
            o = Olimpiada(id_olimpiada=int(row[0]), nombre=row[1], anio=row[2], temporada=row[3], ciudad=row[4])
            session.add(o)

        # INSERTAMOS EVENTOS
        for key in eventos:
            row = eventos[key]
            e = Evento(id_evento=row[0], nombre=row[1], id_olimpiada=row[2], id_deporte=row[3])
            session.add(e)

        # INSERTAMOS PARTICIPACIONES

        for key in participaciones:
            row = participaciones[key]
            p = Participacion(id_deportista=row[0], id_evento=row[1], id_equipo=row[2], edad=row[3], medalla=row[4])
            session.add(p)

        session.commit()


# FUNCIÓN PARA LISTAR LOS DEPORTISTAS QUE PARTICIPAN EN MÁS DE UN DEPORTE
def listarDeportistaDiferenteDeporte(mycursor):
    sql = "SELECT deportista.nombre, deportista.sexo, deportista.peso, deportista.altura, Deporte.nombre as deporte, " \
          "Participacion.edad, Equipo.nombre as equipo, Olimpiada.nombre as olimpiada, " \
          "Participacion.medalla, Evento.nombre as evento " \
          "FROM Deportista deportista, Participacion, Evento, Deporte, Equipo, Olimpiada " \
          "WHERE deportista.id_deportista = Participacion.id_deportista " \
          "AND Equipo.id_equipo = Participacion.id_equipo " \
          "AND Deporte.id_deporte = Evento.id_deporte " \
          "AND Participacion.id_evento = Evento.id_evento " \
          "AND Evento.id_olimpiada = Olimpiada.id_olimpiada " \
          "AND 1 < (" \
          "     SELECT count(distinct Evento.id_deporte) " \
          "     FROM Evento, Participacion " \
          "     WHERE Evento.id_evento =  Participacion.id_evento " \
          "     AND Participacion.id_deportista = deportista.id_deportista" \
          ") " \
          "order by deportista.nombre"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)


# FUNCIÓN PARA LISTAR LOS DEPORTISTAS QUE PARTICIPAN EN UN DEPORTE INTRODUCIDO DE UNA OLIMPIADA INTRODUCIDA DE UNA
# TEMPORADA INTRODUCIDA
def listarDeportistasParticipantes(db, s):
    cursor = db.cursor()

    temporada = introducirTemporada()

    olimpiada = introducirOlimpiada(cursor, temporada)

    deporte = introducirDeporteOlimpiada(cursor, olimpiada)

    evento = introducirEvento(cursor, deporte, olimpiada, s)

    print("Deportistas participantes: \n")
    query = "select Deportista.nombre, altura, peso, edad, Equipo.nombre, medalla " \
            "from Participacion, Deportista, Equipo " \
            "where '" + str(evento) + "' = id_evento " \
                                      "and Participacion.id_deportista = Deportista.id_deportista " \
                                      "and Participacion.id_equipo = Equipo.id_equipo " \
                                      "order by Deportista.nombre;"

    cursor.execute(query)
    myresult = cursor.fetchall()
    contResultDep = 1
    for row in myresult:
        print(str(contResultDep) + ". " + str(row[0]) +
              "\n\t-Altura:" + str(row[1]) +
              "\n\t-Peso:" + str(row[2]) +
              "\n\t-Edad:" + str(row[3]) +
              "\n\t-Equipo:" + str(row[4]) +
              "\n\t-Medalla:" + str(row[5]) +
              "\n")
        contResultDep += 1


# FUNCIÓN PARA INTRODUCIR UNA OLIMPIADA
def introducirDeporteOlimpiada(cursor, olimpiada, s):
    deportes = listarDeportesOlimpada(cursor, s, olimpiada)
    deporte = input("Introduce el id de un deporte: ")
    while deporte not in deportes:
        deporte = input("Introduce un id válido: ")
    return deporte


# FUNCIÓN PARA INTRODUCIR UNA TEMPORADA
def introducirTemporada():
    temporada = input("En que temporada buscamos? (W/S)")
    while (temporada.lower() != "w") and (temporada.lower() != "s"):
        temporada = input("Introduzca una temporada correcta (W/S)")

    temporada = 'Summer' if temporada.lower() == 's' else 'Winter'
    return temporada


# FUNCIÓN PARA LISTAR LAS EDICIONES DE UNA TEMPORADA PASADA POR PARAMETRO
def listarEdiciones(mycursor, temp):
    print("Olimpiadas de la temporada " + temp + ":")
    olimpiadas = session.query(Olimpiada).filter(Olimpiada.temporada == temp).all()

    for olimp in olimpiadas:
        print("\tid: " + olimp.id_olimpiada + " año: " + olimp.anio + " ciudad: " + olimp.ciudad)

    return olimpiadas


# FUNCIÓN PARA LISTAR LOS DEPORTES DE UNA OLIMPIADA PASADA POR PARAMETRO
# EN SQLITE NO FUNCIONA, No sabemos por que
def listarDeportesOlimpada(mycursor, s, olimp):
    mydb = sqlite3.connect("../Ejercicios_UD3/sqlite.db")
    cursor = mydb.cursor()
    print("Deportes de la olimpiada " + olimp + ":")
    sql = "SELECT distinct Deporte.id_deporte, Deporte.nombre FROM Deporte, Evento WHERE Deporte.id_deporte = Evento.id_deporte AND Evento.id_olimpiada = " + s + " order by Deporte.id_deporte"
    # sql = "SELECT Deporte.id_deporte, Deporte.nombre FROM Deporte, Evento WHERE Deporte.id_deporte = Evento.id_deporte AND Evento.id_olimpiada = " + olimp + ""
    # sql = "SELECT nombre FROM Evento WHERE Evento.id_olimpiada = " + s + ""
    # mycursor.execute(sql, (olimp,))
    cursor.execute(sql, (olimp,))
    # mycursor.execute(sql)
    print(sql, olimp)
    # myresult = mycursor.fetchall()
    myresult = cursor.fetchall()
    ids = []
    for x in myresult:
        print("\tid: " + str(x[0]) + " deporte: " + str(x[1]))
        # print("nombre: " + str(x[0]))
        ids.append(str(x[0]))
    return ids


# FUNCIÓN PARA LISTAR LOS EVENTOS DE UN DEPORTE PASADO POR PARAMETRO EN UNA OLIMPIDADA PASADA POR PARAMETRO
def listarEventosDeporteOlimpiada(mycursor, s, olimp, dep):
    print("Eventos del deporte" + dep + " en la olimpiada " + olimp + ":")
    sql = "SELECT id_evento, nombre FROM Evento WHERE id_olimpiada = " + s + " AND id_deporte = " + s + ";"
    mycursor.execute(sql, (olimp, dep))
    myresult = mycursor.fetchall()
    ids = []
    for x in myresult:
        print("\tid: " + str(x[0]) + " nombre: " + str(x[1]))
        ids.append(str(x[0]))
    return ids


# FUNCIÓN PARA LISTAR LOS DEPORTISTAS EN BASE A UN NOMBRE PASADO POR PARAMETRO
def listarDeportistaPorNombre(mycursor, s, nombre):
    print("Deportistas: ")
    sql = "select id_deportista, nombre, altura, peso, sexo from Deportista where nombre like " + s
    mycursor.execute(sql, ("%" + nombre + "%",))
    myresult = mycursor.fetchall()
    ids = []
    for x in myresult:
        print("\tid_deportista: " + str(x[0])
              + " nombre: " + str(x[1])
              + " altura: " + str(x[2])
              + " peso: " + str(x[3])
              + " sexo: " + x[4])
        ids.append(str(x[0]))
    return ids


# FUNCIÓN PARA LISTAR LAS PARTICIPACIONES DE UN DEPORTISTA PASADO POR PARAMETRO
def listarParticipacionesDeportista(mycursor, s, id_deportista):
    print("Participaciones: ")
    sql = "SELECT Participacion.id_deportista, Participacion.id_evento, Evento.nombre as evento, " \
          "Equipo.nombre as equipo, Participacion.edad, Participacion.medalla " \
          "FROM Participacion, Evento, Equipo " \
          "where id_deportista = " + s + "" \
                                         "AND Evento.id_evento = Participacion.id_evento " \
                                         "AND Equipo.id_equipo = Participacion.id_equipo"

    mycursor.execute(sql, (id_deportista,))
    myresult = mycursor.fetchall()
    ids = []
    cont = 0
    for x in myresult:
        print("\t"
              + "id: " + str(cont)
              + " Evento: " + str(x[2])
              + " Equipo: " + str(x[3])
              + " Edad: " + str(x[4])
              + " Medalla: " + str(x[5]))
        ids.append([str(x[0]), str(x[1])])
        cont += 1
    return ids


# FUNCIÓN PARA CAMBIAR LA MEDALLA DE UNA PARTICIPACION (Parametros: el deportista, el vento, la medalla a introducir)
def cambiarMedalla(mycursor, s, deportista, evento, medalla):
    sql = "UPDATE Participacion SET medalla = " + s + " WHERE id_deportista = " + s + " AND id_evento = " + s
    mycursor.execute(sql, (medalla, deportista, evento))


# FUNCIÓN PARA INSERTAR UN DEPORTISTA PASANDOLE EL NOMRBE DE ESTE
def insertDeportista(cursor, s, nombre):
    print("Vamos a introducir un nuevo deportista")
    sql = "INSERT INTO Deportista (nombre, sexo, peso, altura) VALUES (" + s + ", " + s + ", " + s + ", " + s + ")"
    sexo = input("Introduce el sexo (M/F) ")
    while sexo not in ('M', 'F'):
        sexo = input("Introduce un sexo válido (M/F) ")

    peso = int(input("Introduce el peso (Kg) "))
    while peso < 20 or peso > 500:
        peso = int(input("Introduce un peso válido (Kg) "))

    altura = int(input("Introduce la altura (cm) "))
    while altura < 20 or altura > 350:
        altura = int(input("Introduce una altura válida (cm) "))

    cursor.execute(sql, (nombre, sexo, peso, altura))
    print("Deportista introducido.")
    return cursor.lastrowid


# FUNCIÓN PARA LISTAR TODOS LOS EQUIPOS
def listarEquipos(cursor):
    print("Equipos: ")
    sql = "SELECT id_equipo, nombre, iniciales FROM Equipo"
    cursor.execute(sql)
    myresult = cursor.fetchall()
    ids = []
    for x in myresult:
        print("\tid_equipo: " + str(x[0])
              + " nombre: " + str(x[1])
              + " iniciales: " + str(x[2]))
        ids.append(str(x[0]))
    return ids


# FUNCIÓN PARA INSERTAR UNA PARTICIPACIÓN DE UN DEPORTISTA INTRODUCIDO POR PARAMETRO
def insertParticipacion(cursor, s, id_deportista):
    print("Vamos a introducir una participacion")
    temporada = introducirTemporada()

    olimpiada = introducirOlimpiada(cursor, s, temporada)

    deporte = introducirDeporteOlimpiada(cursor, olimpiada, s)

    evento = introducirEvento(cursor, deporte, olimpiada, s)

    equipos = listarEquipos(cursor)
    equipo = input("Introduce el id del equipo: ")
    while equipo not in equipos:
        equipo = input("Introduce un id válido: ")

    sql = "INSERT INTO Participacion (id_deportista, id_evento, id_equipo, edad, medalla) " \
          "VALUES (" + s + ", " + s + ", " + s + ", " + s + ", " + s + ")"

    edad = int(input("Introduce la edad: "))
    while edad < 13 or edad > 120:
        edad = int(input("Introduce una edad válida: "))

    medalla = introducirMedalla()

    cursor.execute(sql, (deportista, evento, equipo, edad, medalla))
    print("Participación introducida.")


# FUNCIÓN PARA INTRODUCIR UN EVENTO DE UN DEPORTE Y UNA OLIMPIADA INTRODUCIDAS POR PARAMETROS
def introducirEvento(cursor, deporte, olimpiada, s):
    eventos = listarEventosDeporteOlimpiada(cursor, s, olimpiada, deporte)
    evento = input("Introduce el id de un evento: ")
    while evento not in eventos:
        evento = input("Introduce un id válido: ")
    return evento


# FUNCIÓN PARA INTRODUCIR UNA OLIMPIADA DE UNA TEMPORADA PASADA POR PARAMETRO
def introducirOlimpiada(cursor, temporada):
    olimpiadas = listarEdiciones(cursor, temporada)
    olimpiada = input("Introduce el id de una olimpiada: ")
    while olimpiada not in olimpiadas:
        olimpiada = input("Introduce un id válido: ")
    return olimpiadas


# FUNCIÓN PARA BORRAR UNA PARTICIPACIÓN
def borrarParticipacion(cursor, s, deportista, evento):
    sql = "DELETE FROM Participacion WHERE id_deportista = " + s + " AND id_evento = " + s
    cursor.execute(sql, (deportista, evento))
    print("Participación eliminada")


opc = "-1"


# FUNCIÓN PARA BUSCAR UN DEPORTISTA POR SU NOMBRE
def buscarDeportistaPorNombre():
    nombre = input("Introduce el nombre de un deportista: ")
    deportistas = listarDeportistaPorNombre(cursor, s, nombre)
    deportista = input("Introduce el id del deportista: ")
    while deportista not in deportistas:
        deportista = input("Introduce un id válido: ")
    return deportista


# FUNCIÓN PARA INTRODUCIR UNA MEDALLA
def introducirMedalla():
    medalla = input("Que medalla le quieres introducir?(Gold/Silver/Bronze/None) ")
    medallas = ['Gold', 'Silver', 'Bronze', 'None']
    while medalla not in medallas:
        medalla = input("Introduce una medalla válida. (Gold/Silver/Bronze/None) ")
    if medalla == 'None':
        medalla = None

    return medalla


# MENÚ CON LAS OPCIONES DEL PROGRAMA
while opc != "0":
    if opc == "1":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()
        insertarDatos(session)
    elif opc == "2":
        db = getSQLiteConnection()
        Session = sessionmaker(bind=db)
        session = Session()
        insertarDatos(session)
    elif opc == "3":
        db, s = seleccionarBD()
        print("Deportistas que han participado en diferentes deportes:")
        listarDeportistaDiferenteDeporte(db.cursor())
        db.close()

    elif opc == "4":
        db = seleccionarBD()
        listarDeportistasParticipantes(db)
        db.close()

    elif opc == "5":
        db, s = seleccionarBD()

        cursor = db.cursor()

        deportista = buscarDeportistaPorNombre()

        participaciones = listarParticipacionesDeportista(cursor, s, deportista)
        participacion = input("Introduce el id de la participacion: ")
        while int(participacion) >= len(participaciones) or int(participacion) < 0:
            participacion = input("Introduce un id válido: ")

        deportista = participaciones[int(participacion)][0]
        evento = participaciones[int(participacion)][1]

        medalla = introducirMedalla()

        cambiarMedalla(cursor, s, deportista, evento, medalla)
        db.close()

    elif opc == "6":
        db, s = seleccionarBD()

        cursor = db.cursor()
        nombre = input("Introduce el nombre de un deportista, si no encontramos uno, se añadirá: ")
        deportistas = listarDeportistaPorNombre(cursor, s, nombre)
        if len(deportistas) == 0:
            deportista = insertDeportista(cursor, s, nombre)
        else:
            deportista = input("Introduce el id del deportista: ")
            while deportista not in deportistas:
                deportista = input("Introduce un id válido: ")

        insertParticipacion(cursor, s, deportista)
        db.close()

    elif opc == "7":
        db, s = seleccionarBD()
        cursor = db.cursor()

        deportista = buscarDeportistaPorNombre()

        participaciones = listarParticipacionesDeportista(cursor, s, deportista)
        participacion = input("Introduce el id de la participacion: ")
        while int(participacion) >= len(participaciones) or int(participacion) < 0:
            participacion = input("Introduce un id válido: ")

        deportista = participaciones[int(participacion)][0]
        evento = participaciones[int(participacion)][1]

        borrarParticipacion(cursor, s, deportista, evento)
        db.close()

    opc = input(
        """¿Que desea hacer?
        1. Insertar los datos en mysql
        2. Insertar los datos en sqlite
        3. Listado de deportistas en diferentes deportes
        4. Listado de deportistas participantes
        5. Modificar medalla deportista
        6. Añadir deportista/participacion
        7. Eliminar participación
        """)
