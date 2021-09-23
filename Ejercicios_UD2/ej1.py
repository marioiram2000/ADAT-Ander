import os
from shutil import copyfile, move


def createDir(r, nombre):
    if os.path.exists(r):
        path = os.path.join(r, nombre)
        os.mkdir(path)


def listarDir(ruta):
    if os.path.exists(ruta):
        dirs = os.listdir(ruta)
        for file in dirs:
            print(file)


def copyFile(ruta, dest):
    if os.path.exists(ruta):
        if dest[-1] != "/":
            dest += "/"
        dest += ruta.split("/")[-1]
        copyfile(ruta, dest)


def moveFile(ruta, dest):
    if os.path.exists(ruta):
        if dest[-1] != "/":
            dest += "/"
        dest += ruta.split("/")[-1]
        # os.rename(ruta, dest)
        # os.replace(ruta, dest)
        move(ruta, dest)


def delete(ruta):
    if os.path.exists(ruta):
        if os.path.isfile("bob.txt"):
            os.remove(ruta)
        else:
            if len(os.listdir(ruta)) == 0:
                os.rmdir(ruta)


opc = "0"
while opc != "6":
    if opc == "1":
        print("Introduce la ruta en la que quieres crear el directorio:")
        ruta = input()
        print("Introduce en nombre del directorio:")
        nombre = input()
        createDir(ruta, nombre)
        print("Directorio creado: "+ruta+"/"+nombre)

    if opc == "2":
        print("Introduce la ruta del directorio que quieres listar:")
        ruta = input()
        listarDir(ruta)

    if opc == "3":
        print("Introduce la ruta completa del archivo que quieres copiar:")
        ruta = input()
        print("Introduce la ruta en la que quieres copiar el archivo:")
        dest = input()
        copyFile(ruta, dest)

    if opc == "4":
        print("Introduce la ruta completa del archivo que quieres mover:")
        ruta = input()
        print("Introduce la ruta a la que quieres mover el archivo:")
        dest = input()
        moveFile(ruta, dest)
    if opc == "5":
        print("")
    if opc != "6":
        print("""¿Que desea hacer?
                    1.  Crear un directorio
                    2.  Listar un directorio
                    3.  Copiar un archivo
                    4.  Mover un archivo
                    5.  Eliminar un archivo/directorio
                    6.  Salir """)
        opc = input()

