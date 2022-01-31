xquery version "3.1";

for $socio in /SOCIOS_GIM/fila_socios
    let $cuota_adic := sum(/CUOTA_SOCIOS/datos[COD=$socio/COD]/cuota_adicional/text())
    let $cuota_fija := $socio/CUOTA_FIJA/text()
    let $cuota_final := $cuota_adic+ $cuota_fija
    return 
    <datos>
        <COD>{$socio/COD/text()}</COD>
        <NOMBRESOCIO>{$socio/NOMBRE/text()}</NOMBRESOCIO>
        <CUOTA_FIJA>{$cuota_fija}</CUOTA_FIJA>
        <suma_cuota_adic>{$cuota_adic}</suma_cuota_adic>
        <cuota_total>{$cuota_final}</cuota_total> 
    </datos>