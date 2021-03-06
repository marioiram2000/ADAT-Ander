from csv import DictReader
import mysql.connector
import sqlite3

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, PrimaryKeyConstraint, update
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
    deporte = relationship("Deporte", back_populates="eventos")


Olimpiada.eventos = relationship("Evento", back_populates="olimpiada")
Deporte.eventos = relationship("Evento", back_populates="deporte")


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


# FUNCI??N PARA BORRAR LAS TABLAS
def borrarTablas(session):
    session.execute("DELETE FROM Participacion;")
    session.execute("DELETE FROM Evento;")
    session.execute("DELETE FROM Olimpiada;")
    session.execute("DELETE FROM Equipo;")
    session.execute("DELETE FROM Deportista;")
    session.execute("DELETE FROM Deporte;")


# FUNCI??N PARA CREAR LAS TABLAS
def crearTablas(session):
    session.execute("CREATE TABLE Deporte (id_deporte, nombre);")
    session.execute("CREATE TABLE Deportista (id_deportista, nombre, sexo, peso, altura);")
    session.execute("CREATE TABLE Equipo (id_equipo, nombre, iniciales);")
    session.execute("CREATE TABLE Evento (id_evento, nombre, id_olimpiada, id_deporte);")
    session.execute("CREATE TABLE Olimpiada (id_olimpiada, nombre, anio, temporada, ciudad);")
    session.execute("CREATE TABLE Participacion (id_deportista, id_evento, id_equipo, edad, medalla);")


# FUNCI??N PARA SELECCIONAR LA BD
def seleccionarBD():
    bbdd = input("??Que base de datos desea usar? (MySQL/SQLite)")
    while (bbdd.lower() != "mysql") and (bbdd.lower() != "sqlite"):
        bbdd = input("Introduzca una base de datos correcta (MySQL/SQLite)")
    if bbdd.lower() == "mysql":
        db = getMySQLConnection()
    else:
        db = getSQLiteConnection()

    Session = sessionmaker(bind=db)
    session = Session()
    return session


# FUNCI??N DE INSERCI??N DE DATOS
def insertarDatos(session):
    # BORRAMOS LAS TABLAS PARA UNA CORRECTA INSERCI??N DE LOS DATOS
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

    # GENERO CLAVES QUE IR?? INCREMENTANDO PARA LOS VALORES QUE NO TIENEN ID
    idDeporteAI = 1
    idEquipoAI = 1
    idEventoAI = 1
    idOlimpiadaAI = 1
    idParticipacionAI = 1

    # NECESITO OTRA VARIABLE PARA ALMACENAR EL ID DE LOS CAMPOS CORRESPONDIENTES DE LA LINEA EN LA QUE EST??
    idDeportista = 1
    idDeporte = idDeporteAI
    idEquipo = idEquipoAI
    idEvento = idEventoAI
    idOlimpiada = idOlimpiadaAI

    # LEEMOS EL FICHERO CSV LINEA POR LINEA
    with open('../Ejercicios_UD3/data/athlete_events.csv', 'r') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            idDeportista = row['ID']  # EL ID DE LA LINEA ES EL DEL DEPORTISTA QUE HACE ESA PARTICIPACI??N

            # SI NO TENGO EL DEPORTE EN LA LISTA, LO A??ADO, SI LO TENGO, COJO EL ID DE ESE DEPORTE
            if row['Sport'] not in deportes:
                deportes[row['Sport']] = [idDeporteAI, row['Sport']]
                idDeporte = idDeporteAI
                idDeporteAI += 1
            else:
                idDeporte = deportes[row['Sport']][0]

            # SI NO TENGO EL DEPORTISTA EN LA LISTA, LO A??ADO. COMPRUEBO LOS VALORES QUE PUEDEN SER 'NA'
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

            # SI NO TENGO EL EQUPIO EN LA LISTA, LO A??ADO, SI LO TENGO, COJO EL ID DE ESE EQUIPO
            if row['Team'] not in equipos:
                equipos[row['Team']] = [idEquipoAI, row['Team'], row['NOC']]
                idEquipo = idEquipoAI
                idEquipoAI += 1
            else:
                idEquipo = equipos[row['Team']][0]

            # SI NO TENGO LA OLIMPIADA EN LA LISTA, LA A??ADO, SI LA TENGO, COJO EL ID DE ESA OLIMPIADA
            if row['Games'] not in olimpiadas:
                olimpiadas[row['Games']] = [idOlimpiadaAI, row['Games'], row['Year'], row['Season'], row['City']]
                idOlimpiada = idOlimpiadaAI
                idOlimpiadaAI += 1
            else:
                idOlimpiada = olimpiadas[row['Games']][0]

            # ENLAZO EL NOMBRE DEL EVENTO CON LA OLIMPIADA PARA GENERAR LA KEY DEL DICCIONARIO, LOS EVENTOS PUEDEN TENER
            # EL MISMO NOMBRE (P.E Basketball Men's Basketball) PARA DIFERENTES OLIMPIADAS.
            # SI NO LO TENGO, LO A??ADO, SI LO TENGO, COJO EL ID.
            # PARA LOS EVENTOS SE NECESITAN LOS IDS QUE HEMOS IDO RECOGIENDO
            if row['Event'] + "_" + str(idOlimpiada) not in eventos:
                eventos[row['Event'] + "_" + str(idOlimpiada)] = [idEventoAI, row['Event'], idOlimpiada, idDeporte]
                idEvento = idEventoAI
                idEventoAI += 1
            else:
                idEvento = eventos[row['Event'] + "_" + str(idOlimpiada)][0]

            # CADA LINEA DEL ARCHIVO ES UNA PARTICIPACI??N, NO NECESITO COMPROBAR SI LO TENGO O NO.
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

            idParticipacionAI += 1  # CLAVE DE PARTICIPACI??N Y CONTADOR DE LA LINEA

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


# FUNCI??N PARA LISTAR LOS DEPORTISTAS QUE PARTICIPAN EN UN DEPORTE INTRODUCIDO DE UNA OLIMPIADA INTRODUCIDA DE UNA
# TEMPORADA INTRODUCIDA
def listarDeportistasParticipantes(session):
    temporada = introducirTemporada()

    olimpiada = introducirOlimpiada(session, temporada)

    deporte = introducirDeporteOlimpiada(session, olimpiada)

    evento = introducirEvento(session, deporte, olimpiada)

    print(evento)
    print("Deportistas participantes: \n")
    contResultDep = 1
    for participacion in session.query(Participacion).filter(Participacion.id_evento == evento):
        print(str(contResultDep) + ". " + str(participacion.deportista.nombre) +
              "\n\t-Altura:" + str(participacion.deportista.altura) +
              "\n\t-Peso:" + str(participacion.deportista.peso) +
              "\n\t-Edad:" + str(participacion.edad) +
              "\n\t-Equipo:" + str(participacion.equipo.nombre) +
              "\n\t-Medalla:" + str(participacion.medalla) +
              "\n")
        contResultDep += 1


# FUNCI??N PARA INTRODUCIR UNA OLIMPIADA
def introducirDeporteOlimpiada(session, olimpiada):
    deportes = listarDeportesOlimpada(session, olimpiada)
    deporte = input("Introduce el id de un deporte: ")
    while deporte not in deportes:
        deporte = input("Introduce un id v??lido: ")
    return deporte


# FUNCI??N PARA INTRODUCIR UNA TEMPORADA
def introducirTemporada():
    temporada = input("En que temporada buscamos? (W/S)")
    while (temporada.lower() != "w") and (temporada.lower() != "s"):
        temporada = input("Introduzca una temporada correcta (W/S)")

    temporada = 'Summer' if temporada.lower() == 's' else 'Winter'
    return temporada


# FUNCI??N PARA LISTAR LAS EDICIONES DE UNA TEMPORADA PASADA POR PARAMETRO
def listarEdiciones(session, temp):
    print("Olimpiadas de la temporada " + temp + ":")
    olimpiadas = []
    for olimp in session.query(Olimpiada).filter(Olimpiada.temporada == temp).all():
        print("\tid: " + str(olimp.id_olimpiada) + " a??o: " + str(olimp.anio) + " ciudad: " + str(olimp.ciudad))
        olimpiadas.append(str(olimp.id_olimpiada))
    return olimpiadas


# FUNCI??N PARA LISTAR LOS DEPORTES DE UNA OLIMPIADA PASADA POR PARAMETRO
def listarDeportesOlimpada(session, olimp):
    print("Deportes de la olimpiada " + olimp + ":")
    olimpiada = session.query(Olimpiada).get(olimp)
    deportes = []
    for evento in olimpiada.eventos:
        if evento.deporte not in deportes:
            deportes.append(evento.deporte)

    ids = []
    for deporte in deportes:
        print("\tid: " + str(deporte.id_deporte) + " deporte: " + deporte.nombre)
        ids.append(str(deporte.id_deporte))
    return ids


# FUNCI??N PARA LISTAR LOS EVENTOS DE UN DEPORTE PASADO POR PARAMETRO EN UNA OLIMPIDADA PASADA POR PARAMETRO
def listarEventosDeporteOlimpiada(session, olimp, dep):
    print("Eventos del deporte " + dep + " en la olimpiada " + olimp + ":")
    ids = []
    for evento in session.query(Evento).filter(Evento.id_olimpiada == olimp, Evento.id_deporte == dep):
        print("\tid: " + str(evento.id_evento) + " nombre: " + str(evento.nombre))
        ids.append(str(evento.id_evento))
    return ids


# FUNCI??N PARA LISTAR LOS DEPORTISTAS EN BASE A UN NOMBRE PASADO POR PARAMETRO
def listarDeportistaPorNombre(session, nombre):
    print("Deportistas: ")
    ids = []
    for deportista in session.query(Deportista).filter(Deportista.nombre.like("%" + nombre + "%")):
        print("\tid_deportista: " + str(deportista.id_deportista)
              + " nombre: " + str(deportista.nombre)
              + " altura: " + str(deportista.altura)
              + " peso: " + str(deportista.peso)
              + " sexo: " + deportista.sexo)
        ids.append(str(deportista.id_deportista))
    return ids


# FUNCI??N PARA LISTAR LAS PARTICIPACIONES DE UN DEPORTISTA PASADO POR PARAMETRO
def listarParticipacionesDeportista(session, id_deportista):
    print("Participaciones: ")
    ids = []
    cont = 0
    for participacion in session.query(Participacion).filter(Participacion.id_deportista == id_deportista):
        print("\t"
              + "id: " + str(cont)
              + " Evento: " + str(participacion.evento.nombre)
              + " Equipo: " + str(participacion.equipo.nombre)
              + " Edad: " + str(participacion.edad)
              + " Medalla: " + str(participacion.medalla))
        ids.append([str(participacion.id_deportista), str(participacion.id_evento)])
        cont += 1
    return ids


# FUNCI??N PARA CAMBIAR LA MEDALLA DE UNA PARTICIPACION (Parametros: el deportista, el vento, la medalla a introducir)
def cambiarMedalla(session, deportista, evento, medalla):
    print(type(deportista), type(evento), medalla)
    session.query(Participacion).get((int(deportista), int(evento))).medalla = medalla
    print(participacion)
    # participacion.medalla = medalla
    session.commit()
    # session.refresh()


# FUNCI??N PARA INSERTAR UN DEPORTISTA PASANDOLE EL NOMRBE DE ESTE
def insertDeportista(session, nombre):
    print("Vamos a introducir un nuevo deportista")

    sexo = input("Introduce el sexo (M/F) ")
    while sexo not in ('M', 'F'):
        sexo = input("Introduce un sexo v??lido (M/F) ")

    peso = int(input("Introduce el peso (Kg) "))
    while peso < 20 or peso > 500:
        peso = int(input("Introduce un peso v??lido (Kg) "))

    altura = int(input("Introduce la altura (cm) "))
    while altura < 20 or altura > 350:
        altura = int(input("Introduce una altura v??lida (cm) "))

    dep = Deportista(nombre=nombre, sexo=sexo, peso=peso, altura=altura)
    session.add(dep)
    session.commit()

    print("Deportista introducido." + str(dep.id_deportista))
    return dep.id_deportista


# FUNCI??N PARA LISTAR TODOS LOS EQUIPOS
def listarEquipos(session):
    print("Equipos: ")
    ids = []
    for equipo in session.query(Equipo):
        print("\tid_equipo: " + str(equipo.id_equipo)
              + " nombre: " + str(equipo.nombre)
              + " iniciales: " + str(equipo.iniciales))
        ids.append(str(equipo.id_equipo))
    return ids


# FUNCI??N PARA INSERTAR UNA PARTICIPACI??N DE UN DEPORTISTA INTRODUCIDO POR PARAMETRO
def insertParticipacion(session, id_deportista):
    print("Vamos a introducir una participacion")
    temporada = introducirTemporada()

    olimpiada = introducirOlimpiada(session, temporada)

    deporte = introducirDeporteOlimpiada(session, olimpiada)

    evento = introducirEvento(session, deporte, olimpiada)

    equipos = listarEquipos(session)
    equipo = input("Introduce el id del equipo: ")
    while equipo not in equipos:
        equipo = input("Introduce un id v??lido: ")

    edad = int(input("Introduce la edad: "))
    while edad < 13 or edad > 120:
        edad = int(input("Introduce una edad v??lida: "))

    medalla = introducirMedalla()

    participacion = Participacion(id_deportista=id_deportista, id_evento=evento, id_equipo=equipo, edad=edad,
                                  medalla=medalla)
    session.add(participacion)
    session.commit()

    print("Participaci??n introducida.")


# FUNCI??N PARA INTRODUCIR UN EVENTO DE UN DEPORTE Y UNA OLIMPIADA INTRODUCIDAS POR PARAMETROS
def introducirEvento(session, deporte, olimpiada):
    eventos = listarEventosDeporteOlimpiada(session, olimpiada, deporte)
    evento = input("Introduce el id de un evento: ")
    while evento not in eventos:
        evento = input("Introduce un id v??lido: ")
    return evento


# FUNCI??N PARA INTRODUCIR UNA OLIMPIADA DE UNA TEMPORADA PASADA POR PARAMETRO
def introducirOlimpiada(session, temporada):
    olimpiadas = listarEdiciones(session, temporada)
    olimpiada = input("Introduce el id de una olimpiada: ")
    while olimpiada not in olimpiadas:
        olimpiada = input("Introduce un id v??lido: ")
    return olimpiada


# FUNCI??N PARA BORRAR UNA PARTICIPACI??N
def borrarParticipacion(session, deportista, evento):
    participacion = session.query(Participacion).get((deportista, evento))
    session.delete(participacion)
    session.commit()
    print("Participaci??n eliminada")


opc = "-1"


# FUNCI??N PARA BUSCAR UN DEPORTISTA POR SU NOMBRE
def buscarDeportistaPorNombre(session):
    nombre = input("Introduce el nombre de un deportista: ")
    deportistas = listarDeportistaPorNombre(session, nombre)
    deportista = input("Introduce el id del deportista: ")
    while deportista not in deportistas:
        deportista = input("Introduce un id v??lido: ")
    return deportista


# FUNCI??N PARA INTRODUCIR UNA MEDALLA
def introducirMedalla():
    medalla = input("Que medalla le quieres introducir?(Gold/Silver/Bronze/None) ")
    medallas = ['Gold', 'Silver', 'Bronze', 'None']
    while medalla not in medallas:
        medalla = input("Introduce una medalla v??lida. (Gold/Silver/Bronze/None) ")
    if medalla == 'None':
        medalla = None

    return medalla


# MEN?? CON LAS OPCIONES DEL PROGRAMA
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
        session = seleccionarBD()
        listarDeportistasParticipantes(session)

    elif opc == "4":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()

        deportista = buscarDeportistaPorNombre(session)
        participaciones = listarParticipacionesDeportista(session, deportista)
        participacion = input("Introduce el id de la participacion: ")
        while int(participacion) >= len(participaciones) or int(participacion) < 0:
            participacion = input("Introduce un id v??lido: ")

        deportista = participaciones[int(participacion)][0]
        evento = participaciones[int(participacion)][1]

        medalla = introducirMedalla()

        cambiarMedalla(session, deportista, evento, medalla)

    elif opc == "5":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()

        nombre = input("Introduce el nombre de un deportista, si no encontramos uno, se a??adir??: ")
        deportistas = listarDeportistaPorNombre(session, nombre)
        if len(deportistas) == 0:
            deportista = insertDeportista(session, nombre)
        else:
            deportista = input("Introduce el id del deportista: ")
            while deportista not in deportistas:
                deportista = input("Introduce un id v??lido: ")

        insertParticipacion(session, deportista)

    elif opc == "6":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()

        deportista = buscarDeportistaPorNombre(session)

        participaciones = listarParticipacionesDeportista(session, deportista)
        participacion = input("Introduce el id de la participacion: ")
        while int(participacion) >= len(participaciones) or int(participacion) < 0:
            participacion = input("Introduce un id v??lido: ")

        deportista = participaciones[int(participacion)][0]
        evento = participaciones[int(participacion)][1]

        borrarParticipacion(session, deportista, evento)

    opc = input(
        """??Que desea hacer?
        1. Insertar los datos en mysql
        2. Insertar los datos en sqlite
        3. Listado de deportistas participantes
        4. Modificar medalla deportista
        5. A??adir deportista/participacion
        6. Eliminar participaci??n
        """)
