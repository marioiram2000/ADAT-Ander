from pyexistdb import db, patch

# Hacemos la conexión a la bd y si no esxiste la colección, la creamos
patch.request_patching(patch.XMLRpcLibPatch)
db = db.ExistDB("http://admin:dm2@localhost:8080/exist")
if not db.hasCollection("GIMNASIO"):
    db.createCollection("GIMNASIO")

# Cargamos los datos
db.load(open("actividades_gim.xml").read(), "GIMNASIO/actividades_gim.xml")
db.load(open("socios_gim.xml").read(), "GIMNASIO/socios_gim.xml")
db.load(open("uso_gimnasio.xml").read(), "GIMNASIO/uso_gimnasio.xml")

# Realizamos la query intermedia
xql = open("queryCuotaSocios.xql").read()
xml = "<CUOTA_SOCIOS>"
rs = db.executeQuery(xql)
for i in range(db.getHits(rs)):
    xml += db.retrieve_text(rs, i)
xml += "</CUOTA_SOCIOS>"

# Subimos el resultado en un archivo temporal
db.load(xml, "GIMNASIO/temp.xml")

# Realizamos la query final
xql = open("queryCuotaFinal.xql").read()
xml = "<CUOTA_FINAL>"
rs = db.executeQuery(xql)
for i in range(db.getHits(rs)):
    xml += db.retrieve_text(rs, i)
xml += "</CUOTA_FINAL>"

# Subimos el resultado
db.load(xml, "GIMNASIO/cuotaSociosFinal.xml")

# Borramos el archivo con el xml temporal
db.removeDocument("GIMNASIO/temp.xml")

