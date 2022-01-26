import mysql.connector

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Query, Session, sessionmaker

Base = declarative_base()


# Definición de clases

# Clase alumnos, de la tabla alumnos con el dni, el apellido y el nombre, la población, el telefono y las notas (objeto)
class alumnos(Base):
    __tablename__ = 'alumnos'
    DNI = Column(String, primary_key=True)
    APENOM = Column(String)
    POBLA = Column(String)
    TELEF = Column(String)


# Clase asignaturaas, de la tabla asignaturas, con el código, el nombre, la abreviatura y las notas (objeto)
class asignaturas(Base):
    __tablename__ = 'asignaturas'
    COD = Column(Integer, primary_key=True)
    NOMBRE = Column(String)
    ABREVIATURA = Column(String)


# clase notas, de la tabla notas, con el dni del alumno, el codigo de la asignatura, la nota, el alumno (objeto) y la
# asignatura (objeto)
class notas(Base):
    __tablename__ = 'notas'
    DNI = Column(String, ForeignKey('alumnos.DNI'), primary_key=True)
    COD = Column(Integer, ForeignKey('asignaturas.COD'), primary_key=True)
    NOTA = Column(Integer)

    alumno = relationship("alumnos", back_populates="notas")
    asignatura = relationship("asignaturas", back_populates="notas")

    # Se especifica la clave primaria compuesta de el dni y el código
    __table_args__ = (
        PrimaryKeyConstraint(DNI, COD),
        {},
    )


alumnos.notas = relationship("notas", back_populates="alumno")
asignaturas.notas = relationship("notas", back_populates="asignatura")


# Función para conectarse a la base de datos
def getMySQLConnection():
    return create_engine("mysql+pymysql://ex2:adat@172.20.132.100/examen2", echo=False)


# Función para listar los alumnos con sus notas de cada asignatura
# Se obtienen ordenados por nombre de forma descendiente
def getAlumnos(session):
    sql = "SELECT DNI, APENOM FROM alumnos ORDER BY APENOM DESC;"
    for alumno in session.query(alumnos).order_by(alumnos.APENOM.desc()):
        print("\n" + str(alumno.APENOM) + "\n" + "----------------------------------------------------------")
        for nota in alumno.notas:
            print(str(nota.asignatura.ABREVIATURA) + "\t" + "\t" + str(nota.NOTA))


# Función para cambiar el nombre de un alumno, pride los datos necesarios por consola
def modificarNombreAlumno(session):
    dni = input("Introduce el DNI del alumno: \n")
    alumno = session.query(alumnos).get(dni)
    while alumno is None:
        print("No hay ningún alumno con ese DNI, vuelva a probar")
        dni = input("Introduce el DNI del alumno: \n")
        alumno = session.query(alumnos).get(dni)

    print("El nombre de ese alumno es el siguiente: " + alumno.APENOM)
    newNombre = input("Escribe el nuevo nombre para el alumno: \n")
    if newNombre == "":
        return

    alumno.APENOM = newNombre
    session.commit()
    print("Alumno modificado correctamente")
    print("Fin de programa.")


# Función para añadir o modificar una nota. Pide los datos por consola.
def aniadirModificarNota(session):
    dni = input("Introduce el DNI del alumno: \n")
    alumno = session.query(alumnos).get(dni)
    while alumno is None:
        print("No hay ningún alumno con ese DNI, vuelva a probar")
        dni = input("Introduce el DNI del alumno: \n")
        alumno = session.query(alumnos).get(dni)

    print(alumno.APENOM)

    print("Listado de asignaturas disponibles:")
    for asignatura in session.query(asignaturas):
        print(str(asignatura.COD) + "-. " + asignatura.NOMBRE + " (" + asignatura.ABREVIATURA + ")")

    cod = input("Esctibe el código de la asignatura a evaluar:\n")
    asignatura = session.query(asignaturas).get(cod)
    while asignatura is None:
        print("No hay nunguna asignatura con ese código, vuelva a probar")
        cod = input("Esctibe el código de la asignatura a evaluar:\n")
        asignatura = session.query(asignaturas).get(cod)

    print(asignatura.ABREVIATURA)
    nota = int(input("Esctibe la nota del alumno:\n"))
    while nota < 0 or nota > 10:
        nota = int(input("Introduce una nota válida:\n"))

    n = session.query(notas).get((dni, cod))
    if n is None:
        n = notas(DNI=dni, COD=cod, NOTA=nota)
        session.add(n)
        session.commit()
        print("La nota se ha añadido")
    else:
        n.NOTA = nota
        session.commit()
        print("La nota se ha modificado")


opc = -1
# MENÚ CON LAS OPCIONES DEL PROGRAMA
while opc != "0":

    if opc == "1":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()

        getAlumnos(session)
        print("")

    elif opc == "2":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()

        modificarNombreAlumno(session)
        print("")

    elif opc == "3":
        db = getMySQLConnection()
        Session = sessionmaker(bind=db)
        session = Session()

        aniadirModificarNota(session)
        print("")

    opc = input(
        """¿Que desea hacer?
        0. Salir
        1. Ver listado de alumnos
        2. Modificar nombre de alumno
        3. Añadir o modificar nota
        """)
