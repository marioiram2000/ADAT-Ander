import pickle
import xml.sax


class olimpiada:
    def __init__(self):
        self.year = ""
        self.juegos = ""
        self.temporada = ""
        self.ciudad = ""

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
while opc != "4":
    if opc == "1":
        Handler = olimpiadasHandler()
        parser = xml.sax.make_parser()
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        parser.setContentHandler(Handler)
        parser.parse("data/olimpiadas.xml")
        olimpiadas = Handler.olimpiadas
        data_string = pickle.dumps(olimpiadas)
        with open('data/objetosOlimpiada', 'wb') as handle:
            pickle.dump(data_string, handle, protocol=pickle.HIGHEST_PROTOCOL)

    if opc == "2":
        print()

    if opc == "3":
        print()

    if opc == "4":
        print()

    if opc != "5":
        print("""Que desea hacer?
            1. Crear fichero serializable de olimpiadas
            2. Añadir edición olímpica
            3. Buscar olimpiadas por sede
            4. Eliminar edición olímpica
            5. Salir""")
        opc = input()
