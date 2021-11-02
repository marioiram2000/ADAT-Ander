import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
import xml.sax
import pickle


class Batalla:
    def __init__(self, id_batalla="", nombre="", anio="", region="", localizacion="", rey_atacante="",
                 rey_defensor="", gana_atacante=""):
        self.id_batalla = id_batalla
        self.nombre = nombre
        self.anio = anio
        self.region = region
        self.localizacion = localizacion
        self.rey_atacante = rey_atacante
        self.rey_defensor = rey_defensor
        self.gana_atacante = gana_atacante

    def __str__(self):
        ganan = "win"
        if self.gana_atacante == "no":
            ganan = "loose"

        return "The " + self.nombre + " took place in " + self.localizacion + " (" + self.region + ") in the year " + \
               self.anio + ". " + "The king(s) " + self.rey_atacante + "fought against " + self.rey_defensor + \
               " and he/they " + ganan


class batallasHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.batallas = []
        self.batalla: Batalla = Batalla()
        self.isInName = False
        self.isInAnio = False
        self.isInRegion = False
        self.isInLocalizacion = False
        self.inInRey = False
        self.isInAtaque = False
        self.isInDefensa = False

    def startElement(self, name, attrs):
        if name == "batalla":
            if attrs.getNames() == ['id']:
                self.batalla.id_batalla = attrs['id']
        if name == "nombre":
            self.isInName = True
        if name == "anio":
            self.isInAnio = True
        if name == "region":
            self.isInRegion = True
        if name == "localizacion":
            self.isInLocalizacion = True
        if name == "ataque":
            self.isInAtaque = True
            self.batalla.gana_atacante = attrs['gana']
        if name == "defensa":
            self.isInDefensa = True
        if name == "rey":
            self.inInRey = True

    def endElement(self, name):
        if name == "nombre":
            self.isInName = False
        if name == "anio":
            self.isInAnio = False
        if name == "region":
            self.isInRegion = False
        if name == "localizacion":
            self.isInLocalizacion = False
        if name == "ataque":
            self.isInAtaque = False
        if name == "defensa":
            self.isInDefensa = False
        if name == "rey":
            self.inInRey = False
        if name == "batalla":
            self.batallas.append(self.batalla)
            self.batalla: Batalla = Batalla()

    def characters(self, content):
        if self.isInName:
            self.batalla.nombre = content
        if self.isInAnio:
            self.batalla.anio = content
        if self.isInRegion:
            self.batalla.region = content
        if self.isInLocalizacion:
            self.batalla.localizacion = content
        if self.isInAtaque:
            if self.inInRey:
                self.batalla.rey_atacante = content
        if self.isInDefensa:
            if self.inInRey:
                self.batalla.rey_defensor = content


def buscarBatallasRegion():
    region = input("Introduce la región: ")

    with open('battles.csv') as archivo:
        reader = csv.reader(archivo)
        isRegion = False
        for linea in reader:
            if linea[23] == region:
                isRegion = True
                resultado = "Gana atacante"
                if linea[13] == "loss":
                    resultado = "Gana defensor"
                print("\n" + linea[23] +
                      "\n\tLocalización: " + linea[22] +
                      "\n\tNombre de la batalla: " + linea[0] +
                      "\n\tAño: " + linea[1] +
                      "\n\tRey atacante: " + linea[3] +
                      "\n\tRey defensor:" + linea[4] +
                      "\n\tResultado: " + resultado)

        if not isRegion:
            print("Lo lamento, no hay batallas en esa región")
    print("\n")


def crearXML():
    with open('battles.csv') as archivo:
        juego_tronos = ET.Element("juego_tronos")
        reader = csv.reader(archivo)
        for linea in reader:
            batalla = ET.SubElement(juego_tronos, 'batalla', id=linea[2])
            ET.SubElement(batalla, "nombre").text = linea[0]
            ET.SubElement(batalla, "anio").text = linea[1]
            ET.SubElement(batalla, "region").text = linea[23]
            localizacion = linea[22]
            if localizacion == "":
                localizacion = "No place"
            ET.SubElement(batalla, "localizacion").text = localizacion

            ganaAtacante = "Si"
            if linea[13] == "loss":
                ganaAtacante = "No"
            ataque = ET.SubElement(batalla, "ataque", tamanio=linea[17], gana=ganaAtacante)
            king = linea[3]
            if king == "":
                king = "No King"
            ET.SubElement(ataque, "rey").text = king
            ET.SubElement(ataque, "comandante").text = linea[19]
            ET.SubElement(ataque, "familia").text = linea[5]
            if linea[6] != "":
                ET.SubElement(ataque, "familia").text = linea[6]
            if linea[7] != "":
                ET.SubElement(ataque, "familia").text = linea[7]
            if linea[8] != "":
                ET.SubElement(ataque, "familia").text = linea[8]

            ganaDefensor = "Si"
            if ganaAtacante == "Si":
                ganaDefensor = "No"
            defensa = ET.SubElement(batalla, "defensa", tamanio=linea[18], gana=ganaDefensor)
            king = linea[4]
            if king == "":
                king = "No King"
            ET.SubElement(defensa, "rey").text = king
            ET.SubElement(defensa, "comandante").text = linea[20]
            ET.SubElement(defensa, "familia").text = linea[9]
            if linea[10] != "":
                ET.SubElement(defensa, "familia").text = linea[10]
            if linea[11] != "":
                ET.SubElement(defensa, "familia").text = linea[11]
            if linea[12] != "":
                ET.SubElement(defensa, "familia").text = linea[12]

        xmlstr = minidom.parseString(ET.tostring(juego_tronos)).toprettyxml(indent="   ")
        with open("battles.xml", "w") as f:
            f.write(xmlstr)
    print("\nFichero XML creado\n")


def crearBinario():
    Handler = batallasHandler()
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    parser.setContentHandler(Handler)
    parser.parse("battles.xml")
    batallas = Handler.batallas
    with open('battles.pickle', 'wb') as handle:
        for batalla in batallas:
            pickle.dump(batalla, handle)
    print("\nFichero binario creado\n")


def eliminarBatalla():
    id_batalla = input("Introduce el identificador de la batalla: ")
    batallas = []
    batallaEncontrada = False

    with open('battles.pickle', 'rb') as openHandler:
        while True:
            try:
                batalla = pickle.load(openHandler)
                if batalla.id_batalla != id_batalla:
                    batallas.append(batalla)
                else:
                    batallaEncontrada = True
                    conf = input("¿DESEA ELIMINAR LA BARALLA? S/N \n")
                    if conf.upper() == "N":
                        batallas.append(batalla)
                    else:
                        print("\nBatalla eliminada\n")
            except EOFError:
                break
    with open('battles.pickle', 'wb+') as writeHandler:
        for batalla in batallas:
            pickle.dump(batalla, writeHandler)
    if not batallaEncontrada:
        print("\nNo se ha encontrado una batalla con ese id\n")


opc = "-1"
while opc != "0":
    if opc == "1":
        buscarBatallasRegion()
    if opc == "2":
        crearXML()
    if opc == "3":
        crearBinario()
    if opc == "4":
        eliminarBatalla()
    if opc != "0":
        print("""¿Que desea hacer?
                    1.  Buscar batallas por región
                    2.  Crear XML de batallas
                    3.  Crear fichero binario de objetos
                    4.  Eliminar batalla del fichero binario de objetos
                    0.  Salir """)
        opc = input()
