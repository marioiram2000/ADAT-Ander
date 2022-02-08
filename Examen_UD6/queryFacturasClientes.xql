for $factura in /facturas/factura
    let $numFatura := data($factura/@numero)
    let $numCliente := $factura/numcliente/text()
    let $nomCliente := /clientes/clien[@numero=$numCliente]/nombre/text()

    return
        <facturasclientes>
            <nombre>{$nomCliente}</nombre>
            <nufact>{$numFatura}</nufact>
        </facturasclientes>