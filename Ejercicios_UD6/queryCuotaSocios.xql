for $uso in /USO_GIMNASIO/fila_uso

    let $nomSocio:=  /SOCIOS_GIM/fila_socios[COD = $uso/CODSOCIO ]/NOMBRE/text()
    let $nomActiv:= /ACTIVIDADES_GIM/fila_actividades[@cod = $uso/CODACTIV ]/NOMBRE/text()
    let $tipoAct:= data(/ACTIVIDADES_GIM/fila_actividades[@cod = $uso/CODACTIV ]/@tipo)
    let $horas := $uso/HORAFINAL/text() - $uso/HORAINICIO/text()
    return 
        <datos>
            <COD>{$uso/CODSOCIO/text()}</COD>
            <NOMBRESOCIO>{$nomSocio}</NOMBRESOCIO>
            <CODACTIV>{data($uso/CODACTIV)}</CODACTIV>
            <NOMBREACTIVIDAD>{$nomActiv}</NOMBREACTIVIDAD>
            <horas>$horas</horas>
            <tipoact>{$tipoAct}</tipoact>
            
            {
            if ($tipoAct=1) then
               <cuota_adicional> 0 </cuota_adicional>
            else if($tipoAct=2) then
                <cuota_adicional> { 2 * $horas } </cuota_adicional>
            else 
                <cuota_adicional> { 4 * $horas }</cuota_adicional>
            }
           
        </datos>
    