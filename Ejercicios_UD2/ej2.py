import csv
import pandas as pd
import os
opc = "0"
while opc != "5":
    if opc == "1":
        df = pd.read_csv("data/athlete_events.csv")[['Games', 'Year', 'Season', 'City']].drop_duplicates()
        df.to_csv("data/olimpiadas.csv")

    elif opc == "2":
        print("Introduce el nombre del deportista")
        name = input()
        df = pd.read_csv("data/athlete_events.csv")
        print(df.loc[df['Name'] == name])

    elif opc == "3":
        try:
            sport = input("En que deporte quieres buscar? ")
            # sport = "Judo"
            print("En que año?")
            year = int(input())
            # year = 1992
            print("De que temporada?(Summer/Winter)")
            season = input()
            # season = "Summer"
            df = pd.read_csv("data/athlete_events.csv")
            data = df.loc[(df['Sport'] == sport) & (df['Year'] == year) & (df['Season'] == season)]
            print(data[['Games', 'City', 'Sport']].iloc[0])
            print(data[['Name', 'Event', 'Medal']])
        except IndexError:
            print("No se ha encontrado nada")

    if opc == "4":
        # "ID","Name","Sex","Age","Height","Weight","Team","NOC","Games","Year","Season","City","Sport","Event","Medal"
        print("Introduzca los datos del deportista")
        df = pd.read_csv("data/athlete_events.csv")
        aid = df.groupby('ID').tail(1)
        name = input("Nombre: ")
        sex = input("Sex: ")
        age = int(input("Age: "))
        height = float(input("Height: "))
        weight = float(input("Weight: "))
        team = input("Team: ")
        noc = input("NOC: ")
        games = input("Games: ")
        year = int(input("Year: "))
        season = input("Season: ")
        city = input("City: ")
        sport = input("Sport: ")
        event = input("Event: ")
        medal = input("Medal: ")
        # row = {"Id": athlete_id, "Name": name, "Sex": sex, "Age": age, "Height": height,
        #       "Weight": weight, "Team": team, "NOC": noc, "Games": games,
        #       "Year": year, "Season": season, "City": city, "Sport": sport,"Event": event,
        #       "Medal": medal}
        df.loc[-1] = [aid, name, sex, age, height, weight, team, noc, games, year, season, city, sport, event, medal]
        print(df.loc[-1])

    if opc != "5":
        print("""Que desea hacer?
                    1. Generar fichero csv de olimpiadas
                    2. Buscar deportista
                    3. Buscar deportistas por deporte y olimpiada
                    4. Añadir deportista
                    5. Salir""")
        opc = input()

