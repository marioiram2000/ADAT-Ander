import pickle
import xml.sax


class olimpiada:
    def __init__(self, year="", juegos="", temporada="", ciudad=""):
        self.year = year
        self.juegos = juegos
        self.temporada = temporada
        self.ciudad = ciudad

    def __str__(self):
        print(self.year + "\t" + self.juegos + "\t" + self.temporada + "\t" + self.ciudad)


class olimpiadasHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.olimpiadas = []
        self.olimpiada: olimpiada = olimpiada()
        self.isJuegos = False
        self.isTemporada = False
        self.isCiudad = False

    def startElement(self, name, attrs):
        if attrs.getNames() == ['year']: self.olimpiada.year = attrs['year']
        if name == "juegos": self.isJuegos = True
        if name == "temporada": self.isTemporada = True
        if name == "ciudad": self.isCiudad = True

    def endElement(self, name):
        if name == "juegos": self.isJuegos = False
        if name == "temporada": self.isTemporada = False
        if name == "ciudad": self.isCiudad = False
        self.olimpiadas.append(self.olimpiada)

    def characters(self, content):
        if self.isCiudad: self.olimpiada.ciudad = content
        if self.isTemporada: self.olimpiada.temporada = content
        if self.isJuegos: self.olimpiada.juegos = content


opc = 0
while opc != "5":
    if opc == "1":
        Handler = olimpiadasHandler()
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        parser.setContentHandler(Handler)
        parser.parse("data/olimpiadas.xml")
        olimpiadas = Handler.olimpiadas
        with open('data/objetosOlimpiada.pickle', 'wb') as handle:
            for olimp in olimpiadas:
                pickle.dump(olimp, handle)

    if opc == "2":
        anio = input("Año: ")
        temporada = input("Temporada: ")
        ciudad = input("Ciudad: ")
        nombre = anio + " " + temporada

        olimp = olimpiada(anio, nombre, temporada, ciudad)
        with open('data/objetosOlimpiada.pickle', 'wb') as handle:
            pickle.dump(olimp, handle)

    if opc == "3":
        sede = input("Introduce la sede: \n")
        with open('data/objetosOlimpiada.pickle', 'rb') as f:
            while True:
                try:
                    olimpiada = pickle.load(f)
                    if (olimpiada.ciudad == sede):
                        print(olimpiada.__str__())
                except EOFError:
                    break

    if opc == "4":
        print("Introduce los datos de la olimpiada");
        ciudad = input("Ciudad: ")
        anio = input("Año: ")
        olimps = []
        with open('data/objetosOlimpiada.pickle', 'rb') as openHandler:
            while True:
                try:
                    olimp = pickle.load(openHandler)
                    if olimp.year != anio and olimp.ciudad != ciudad:
                        olimps.append(olimp)
                except EOFError:
                    break

        with open('data/objetosOlimpiada.pickle', 'wb') as writeHandler:
            for olimp in olimps:
                pickle.dump(olimp, writeHandler)

    if opc != "5":
        print("""Que desea hacer?
            1. Crear fichero serializable de olimpiadas
            2. Añadir edición olímpica
            3. Buscar olimpiadas por sede
            4. Eliminar edición olímpica
            5. Salir""")
        opc = input()
