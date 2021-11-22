import mysql.connector


# Función para conectarse a la base de datos
def getMySQLConnection():
    mydb = mysql.connector.connect(
        host="172.20.132.100",
        user="ex2",
        password="adat",
        database="examen2",
        autocommit=True  # autocommit, para que se inserten los datos directamente
    )
    return mydb


# Función para listar los alumnos con sus notas de cada asignatura
# Se obtienen ordenados por nombre de forma descendiente
def getAlumnos(cursor):
    sql = "SELECT DNI, APENOM FROM alumnos ORDER BY APENOM DESC;"
    cursor.execute(sql)
    myresult = cursor.fetchall()
    for x in myresult:
        print("\n" + str(x[1]) + "\n" + "----------------------------------------------------------")
        getNotasAlumno(cursor, x[0])


# Función que recive el dni de un alumno y lista las notas de este
def getNotasAlumno(cursor, dni):
    sql = "SELECT DNI, COD, NOTA FROM notas WHERE DNI = %s;"
    cursor.execute(sql, (dni,))
    result = cursor.fetchall()
    for x in result:
        abr = getNombreAsignatura(cursor, x[1])
        print(abr + "\t" + "\t" + "\t" + str(x[2]))


# Función que recive el código de una asignatura y devuelve la abreviatura de esta
def getNombreAsignatura(cursor, cod):
    sql = "SELECT ABREVIATURA FROM asignaturas WHERE COD = %s"
    cursor.execute(sql, (cod,))
    result = cursor.fetchall()
    return result[0][0]


# Función para obtener el DNI y el nombre del alumno mediante la inserción por consola del DNI de este
def getAlumnoPorNombre(cursor):
    dni = input("Introduce el DNI del alumno: \n")
    nombre = getNombreAlumno(cursor, dni)
    while nombre == "":
        dni = input("No hemos encontrado un alumno con ese DNI, pruebe con otro: \n")
        nombre = getNombreAlumno(cursor, dni)
    print(nombre)
    return dni, nombre


# Función que devuelve el nombre del alunmo cuyo DNI le pasamos por parametro
def getNombreAlumno(cursor, dni):
    sql = "SELECT APENOM FROM alumnos WHERE DNI = %s"
    cursor.execute(sql, (dni,))
    result = cursor.fetchall()
    if len(result) != 1:
        return ""
    return result[0][0]


# Función para cambiar el nombre de un alumno, pride los datos necesarios por consola
def modificarNombreAlumno(cursor):
    dni, nombre = getAlumnoPorNombre(cursor)
    newNombre = input("Escribe el nuevo nombre para el alumno: \n")
    if newNombre == "":
        return
    sql = "UPDATE alumnos SET APENOM = %s WHERE DNI = %s;"
    cursor.execute(sql, (newNombre, dni))
    print("Alumno modificado correctamente")
    print("Fin de programa.")


# Función para listar las asignaturas con su código, nombre y abreviatura, devuelve un array con los códigos de estas
def listadoAsignaturas(cursor):
    print("Listado de asignaturas disponibles:")
    sql = "SELECT COD, NOMBRE, ABREVIATURA FROM asignaturas;"
    cursor.execute(sql)
    myresult = cursor.fetchall()
    ids = []
    for x in myresult:
        print(str(x[0]) + "-. " + str(x[1]) + " (" + str(x[2]) + ")")
        ids.append(str(x[0]))
    return ids


# Función para actualizar la nota de un alumno. Recive la nota, el dni y el codigo de la asignatura
def updateNota(cursor, nota, dni, cod):
    sql = "UPDATE notas SET NOTA = %s WHERE DNI = %s AND COD = %s;"
    cursor.execute(sql, (nota, dni, cod))
    print("La nota se ha modificado")


# Función para insertar una nota de un alumno. Recive la nota, el dni y el codigo de la asignatura
def insertNota(cursor, nota, dni, cod):
    sql = "INSERT INTO notas (DNI, COD, NOTA) VALUES (%s, %s, %s);"
    cursor.execute(sql, (dni, cod, nota))
    print("La nota se ha añadido")


# Función para añadir o modificar una nota. Pide los datos por consola.
def aniadirModificarNota(cursor):
    dni, nombre = getAlumnoPorNombre(cursor)
    asignaturas = listadoAsignaturas(cursor)
    cod = input("Esctibe el código de la asignatura a evaluar:\n")
    while cod not in asignaturas:
        cod = input("Introude un código válido:\n")
    nota = int(input("Esctibe la nota del alumno:\n"))
    while nota < 0 or nota > 10:
        nota = int(input("Introduce una nota válida:\n"))

    sql = "SELECT DNI, COD, NOTA FROM notas WHERE DNI = %s AND COD = %s"
    cursor.execute(sql, (dni, cod))
    result = cursor.fetchall()
    if len(result) > 0:
        updateNota(cursor, nota, dni, cod)
    else:
        insertNota(cursor, nota, dni, cod)


# Función para añadir o modificar una nota. Pide los datos por consola. Usa una función almacenada en la base de datos
def aniadirModificarNotaFuncion(cursor):
    dni, nombre = getAlumnoPorNombre(cursor)
    asignaturas = listadoAsignaturas(cursor)
    cod = input("Esctibe el código de la asignatura a evaluar:\n")
    while cod not in asignaturas:
        cod = input("Introude un código válido:\n")
    nota = int(input("Esctibe la nota del alumno:\n"))
    while nota < 0 or nota > 10:
        nota = int(input("Introduce una nota válida:\n"))

    args = [cod, dni, nota]
    result_args = cursor.callproc('insertar_nota', args)
    print(result_args[1])


opc = -1
# MENÚ CON LAS OPCIONES DEL PROGRAMA
while opc != "0":
    if opc == "1":
        db = getMySQLConnection()
        getAlumnos(db.cursor())
        db.close
        print("")

    elif opc == "2":
        db = getMySQLConnection()
        modificarNombreAlumno(db.cursor())
        db.close
        print("")

    elif opc == "3":
        db = getMySQLConnection()
        aniadirModificarNota(db.cursor())
        db.close
        print("")

    elif opc == "4":
        db = getMySQLConnection()
        aniadirModificarNota(db.cursor())
        db.close
        print("")

    opc = input(
        """¿Que desea hacer?
        0. Salir
        1. Ver listado de alumnos
        2. Modificar nombre de alumno
        3. Añadir o modificar nota
        4. Añadir o modificar nota (Mediante dunción almacenada en MySQL)
        """)
