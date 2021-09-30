import os

import pandas as pd
import xml.etree.ElementTree as ET

opc = 0
while opc != "4":
    if opc == "1":
        df = pd.read_csv("data/olimpiadas.csv")
        xml_data = ['<root>\n']
        for i in df.index:
            xml_data.append('\t<olimpiada year={}>\n'.format(df[df.columns[1]][i]))  # Opening element tag
            xml_data.append('\t\t<juegos>{}</juegos>\n'.format(df[df.columns[0]][i]))
            xml_data.append('\t\t<temporada>{}</temporada>\n'.format(df[df.columns[2]][i]))
            xml_data.append('\t\t<ciudad>{}</ciudad>\n'.format(df[df.columns[3]][i]))
            xml_data.append('\t</olimpiada>\n')  # Closing element tag
        xml_data.append('</root>')

        with open("data/olimpiadas.xml", 'w') as f:  # Writing in XML file
            for line in xml_data:
                f.write(line)
        os.close("data/olimpiadas.xml")

    if opc == "2":
        df = pd.read_csv("data/athlete_events.csv")
        deportistas = ET.Element('deportistas')
        # ['ID', 'Name', 'Sex', 'Age', 'Height', 'Weight', 'Team', 'NOC', 'Games','Year',
        # 'Season', 'City', 'Sport', 'Event', 'Medal']
        for i in df.index:
            deportista = ET.SubElement(deportistas, 'deportista', id=df[df.columns[0]][i])
            ET.SubElement(deportista, "nombre").text = df[df.columns[1]][i]
            ET.SubElement(deportista, "sexo").text = df[df.columns[2]][i]
            ET.SubElement(deportista, "altura").text = df[df.columns[3]][i]
            ET.SubElement(deportista, "peso").text = df[df.columns[4]][i]
            participaciones = ET.SubElement(deportista, "participaciones")
            #deporte = ET.SubElement(participaciones, "participaciones")

    if opc == "3":
        print("")
    if opc != "4":
        print("""Que desea hacer?
            1. Crear fichero XML de olimpiadas
            2. Crear un fichero XML de deportistas
            3. Listado de olimpiadas
            4. Salir""")
        opc = input()
