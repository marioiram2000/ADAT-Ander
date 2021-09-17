import random


class Persona:
    """Clase persona"""
    def __init__(self, nombre="", edad=0, sexo="hombre", peso=0, altura=0):
        self.LETRAS_CON_NUMEROS = {0: 'T', 1: 'R', 2: 'W', 3: 'A', 4: 'G', 5: 'M', 6: 'Y', 7: 'F', 8: 'P', 9: 'D',
                                   10: 'X', 11: 'B',
                                   12: 'N', 13: 'J', 14: 'Z', 15: 'S', 16: 'Q', 17: 'V', 18: 'H', 19: 'L', 20: 'C',
                                   21: 'H', 22: 'E'}
        self.nombre = nombre
        self.edad = edad
        self.dni = self.generarDni()
        self.sexo = sexo
        self.peso = peso
        self.altura = altura

    def calcularIMC(self):
        try:
            icm = self.peso / (self.altura ** 2)
            if icm < 20:
                return -1
            if icm <= 25:
                return 0
            return 1

        except ZeroDivisionError:
            return -1

    def esMayorDeEdad(self):

        if (self.edad < 18):
            return False

        return True

    def __str__(self):
        return self.nombre + "\t" + str(self.edad) + "\t" + self.sexo + "\t" + str(self.peso) + "\t" + str(self.altura)

    def generarDni(self):
        dni = ""
        for i in range(8):
            dni += str(random.randint(0, 9))
        dni += self.__obtenerLetraDni(dni)
        return dni

    def __obtenerLetraDni(self, dni):
        suma = 0
        for i in dni:
            suma += int(i)
        return self.LETRAS_CON_NUMEROS[suma % 23]


# nombre = "", edad = 0, sexo = "hombre", peso = 0, altura = 0
persona1 = Persona()
persona2 = Persona("mario", edad=21, peso=72, altura=1.84)
persona3 = Persona("miriam", 25, "mujer", 65, 1.74)

# Cosas para la primera persona
if persona1.calcularIMC() == 0:
    print("Persona 1 está en su peso ideal")
elif persona1.calcularIMC() == 1:
    print("Persona 1 tiene sobrepeso")
else:
    print("Persona 1 tiene bajopeso")

print("Es mayor de edad") if persona1.esMayorDeEdad() else print("Es")
print(persona1.__str__())

# Cosas para la segunda persona
if persona2.calcularIMC() == 0:
    print("persona2 está en su peso ideal")
elif persona2.calcularIMC() == 1:
    print("persona2 tiene sobrepeso")
else:
    print("persona2 tiene bajopeso")

print("Es mayor de edad") if persona2.esMayorDeEdad() else print("Es")
print(persona2.__str__())

# Cosas para la tercera persona
if persona3.calcularIMC() == 0:
    print("persona3 está en su peso ideal")
elif persona3.calcularIMC() == 1:
    print("persona3 tiene sobrepeso")
else:
    print("persona3 tiene bajopeso")

print("Es mayor de edad") if persona3.esMayorDeEdad() else print("Es")
print(persona3.__str__())
