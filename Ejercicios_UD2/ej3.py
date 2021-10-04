import os
from builtins import str
from xml.dom import minidom

import pandas as pd
import xml.etree.ElementTree as ET
import xml.sax

class olimpiadasHandler( xml.sax.ContentHandler ):
    def __init__(self):
        self.games = ""
        self.year = ""

    def startElement(self, name, attrs):
        if name == "olimpiada":
            self.buffer += attrs
        if(name == "juegos"):
            self.buffer += name

    def characters(self, content):
        if self.CurrentData == "type":
            self.type = content

opc = 0
while opc != "4":
    if opc == "1":
        df = pd.read_csv("data/olimpiadas.csv")
        xml_data = ['<root>\n']
        for i in df.index:
            xml_data.append('\t<olimpiada year={}>\n'.format(df[df.columns[1]][i]))
            xml_data.append('\t\t<juegos>{}</juegos>\n'.format(df[df.columns[0]][i]))
            xml_data.append('\t\t<temporada>{}</temporada>\n'.format(df[df.columns[2]][i]))
            xml_data.append('\t\t<ciudad>{}</ciudad>\n'.format(df[df.columns[3]][i]))
            xml_data.append('\t</olimpiada>\n')
        xml_data.append('</root>')

        with open("data/olimpiadas.xml", 'w') as f:
            for line in xml_data:
                f.write(line)
        os.close("data/olimpiadas.xml")

    if opc == "2":
        df = pd.read_csv("data/atletas.csv")
        deportistas = ET.Element('deportistas')
        # ['ID', 'Name', 'Sex', 'Age', 'Height', 'Weight', 'Team', 'NOC', 'Games','Year',
        # 'Season', 'City', 'Sport', 'Event', 'Medal']
        id = -1
        deporte = ""
        for i in df.index:
             if id != int(df[df.columns[0]][i]):
                 id = int(df[df.columns[0]][i])
                 deportista = ET.SubElement(deportistas, 'deportista', id=str(id))
                 ET.SubElement(deportista, "nombre").text = df[df.columns[1]][i]
                 ET.SubElement(deportista, "sexo").text = df[df.columns[2]][i]
                 ET.SubElement(deportista, "altura").text = str(df[df.columns[4]][i])
                 ET.SubElement(deportista, "peso").text = str(df[df.columns[5]][i])
                 participaciones = ET.SubElement(deportista, "participaciones")

             if deporte != df[df.columns[12]][i]:
                 deporte = ET.SubElement(participaciones, "deporte", nombre=df[df.columns[12]][i])

             participacion = ET.SubElement(deporte, "participacion", edad=str(df[df.columns[3]][i]))
             ET.SubElement(participacion, "equipo", abbr = df[df.columns[7]][i]).text = df[df.columns[6]][i]
             ET.SubElement(participacion, "juegos").text = str(df[df.columns[8]][i])+" - "+str(df[df.columns[11]][i])
             ET.SubElement(participacion, "evento").text = df[df.columns[13]][i]
             medalla = ""
             if str(df[df.columns[14]][i])!="nan":
                medalla = str(df[df.columns[14]][i])
             ET.SubElement(participacion, "medalla").text = medalla


        xmlstr = minidom.parseString(ET.tostring(deportistas)).toprettyxml(indent="   ")
        with open("data/deportistas.xml", "w") as f:
            f.write(xmlstr)
    if opc == "3":
        Handler = olimpiadasHandler()
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        parser.setContentHandler(Handler)
        parser.parse("movies.xml")
    if opc != "4":
        print("""Que desea hacer?
            1. Crear fichero XML de olimpiadas
            2. Crear un fichero XML de deportistas
            3. Listado de olimpiadas
            4. Salir""")
        opc = input()
