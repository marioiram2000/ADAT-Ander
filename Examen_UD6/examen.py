from pyexistdb import db, patch

# Hacemos la conexión a la bd y si no esxiste la colección, la creamos
patch.request_patching(patch.XMLRpcLibPatch)
db = db.ExistDB("http://admin:dm2@localhost:8080/exist")
if not db.hasCollection("VENTAS"):
    db.createCollection("VENTAS")


# Cargamos los datos
def cargarDatos():
    db.load(open("ColeccionVentas/clientes.xml").read(), "VENTAS/clientes.xml")
    db.load(open("ColeccionVentas/detallefacturas.xml").read(), "VENTAS/detallefacturas.xml")
    db.load(open("ColeccionVentas/facturas.xml").read(), "VENTAS/facturas.xml")
    db.load(open("ColeccionVentas/productos.xml").read(), "VENTAS/productos.xml")
    print("Datos cargados")


# Insertar una factura
def insertarFactura():
    # recogemos los datos
    numFactura = input("Introduce el número de factura: ")
    fecha = input("Introduce la fecha: ")
    importe = input("Introduce el importe: ")
    numCliente = input("Introduce el número de cliente: ")

    # Comprobamos el número de cliente
    xqlCliente = "for $cliente in /clientes/clien return $cliente[@numero = '%s']" % numCliente
    rs = db.executeQuery(xqlCliente)
    if db.getHits(rs) == 0:
        print("No existe ese cliente, me marcho, ahora empieza de nuevo")
        exit()

    xqlInsertFactura = "update insert <factura numero='%s'> <fecha>%s</fecha> <importe>%s</importe> " \
                       "<numcliente>%s</numcliente></factura> into /facturas" % \
                       (numFactura, fecha, importe, numCliente)

    db.executeQuery(xqlInsertFactura)

    # Vamos preparando la insert
    xqlInsertDetalle = "update insert <factura numero='%s'><codigo>FACT%s</codigo>" % (numFactura, numFactura)

    # recogemos más datos
    codProducto = input("Introduce el código del producto (0 para terminar)")
    unidades = input("Introduce las unidades: ")
    xqlProducto = "for $producto in /productos/product return $producto[codigo='%s']" % codProducto
    while codProducto != str(0):
        rs = db.executeQuery(xqlProducto)
        if db.getHits(rs) == 0:
            print("No existe ese producto, me marcho, ahora empieza de nuevo")
            exit()
        xqlInsertDetalle += "<producto descuento='0'><codigo>%s</codigo><unidades>%s</unidades></producto>" % \
                            (codProducto, unidades)
        codProducto = input("Introduce el código del producto (0 para terminar)")
        xqlProducto = "for $producto in /productos/product return $producto[codigo='%s']" % codProducto
        if codProducto != str(0):
            unidades = input("Introduce las unidades: ")

    xqlInsertDetalle += "</factura> into /detallefacturas"
    # Ejecutamos la inserción con los datos introducidos
    db.executeQuery(xqlInsertDetalle)

    print("Factura y detalles introducidos")


# Obtener todas las facturas
def obtenerFacturasClientes():
    # La consulta está guardada en un archio que procedemos a leer
    xql = open("queryFacturasClientes.xql").read()
    xml = "<FACTURAS>" + "\n"
    rs = db.executeQuery(xql)
    for i in range(db.getHits(rs)):
        xml += db.retrieve_text(rs, i) + "\n"
    xml += "</FACTURAS>"
    print(xml)


opc = input("""Que deseas hacer? 
    1- Cargar los datos
    2- Insertar nueva factura
    3- Ver las facturas de los clientes
    0- Salir
    """)
while opc != "0":
    if opc == "1":
        cargarDatos()
    elif opc == "2":
        insertarFactura()
    elif opc == "3":
        obtenerFacturasClientes()

    opc = input("""Que deseas hacer? 
        1- Cargar los datos
        2- Insertar nueva factura
        3- Ver las facturas de los clientes
        0- Salir
        """)