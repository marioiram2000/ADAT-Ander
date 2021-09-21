def suma(l):
    """Devuelve la suma de todos los valores de la colección pasada"""
    return sum(l)
def media(l):
    """Devuelve la media de los valores de la lista"""
    return (sum(l)/len(l))
def maxNum(l):
    """Devuelve el numero más alto de la lista"""
    return max(l)
def minNum(l):
    """Devuelve el numero más bajo de la lista"""
    return min(l)

print("Vas a introducir 10 numeros")
lista = []
while len(lista) < 5:
    print("Introduce el "+str(len(lista)+1)+"º numero")
    num = int(input())
    if num % 2 == 1:
        lista.append(num)
print("Lista completa: "+str(lista))
print("""¿Que desea hacer con la lista?
        0.  Salir
        1.  Sumatorio
        2.  Media
        3.  Máximo
        4.  Mínimo""")
opc = input()
while opc != 0:
    if opc == "1":
        print("Suma de todos los numeros: " + str(suma(lista)))
    elif opc == "2":
        print("Media de todos los numeros: "+str(media(lista)))
    elif opc == "3":
        print("Numero más alto: " + str(maxNum(lista)))
    elif opc == "4":
        print("Numero más bajo: " + str(minNum(lista)))

    if(opc != "0"):
        print("""¿Que desea hacer con la lista?
                0.  Salir
                1.  Sumatorio
                2.  Media
                3.  Máximo
                4.  Mínimo""")
        opc = input()


