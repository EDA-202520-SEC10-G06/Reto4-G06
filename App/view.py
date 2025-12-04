import sys
from tabulate import tabulate # type: ignore
import App.logic as lg
import os

def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    pass

def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    #TODO: Realizar la carga de datos
    pass


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    #TODO: Realizar la función para imprimir un elemento
    pass

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 1
    pass


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    pass


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    res = lg.req_3(control)
    
    if res == None:
        print("\n No se encontró una ruta migratoria viable dentro del nicho")
        return 
    
    print("Requerimiento 3"+
          "\nTotal de puntos en la ruta: "+str(res["total_puntos"])+
          "\nTotal de individuos en la ruta: "+str(res["total_individuos"]))
    
    def table(lista):
        headers = ["ID Punto",
            "Latitud",
            "Longitud",
            "# Individuos",
            "Primeros 3 individuos",
            "Últimos 3 individuos",
            "Distancia anterior",
            "Distancia siguiente"
        ]
        
        filas = []
        for p in lista:
            fila= [
                p["punto_id"],
                p["latitud"],
                p["longitud"],
                p["num_individuos"],
                ", ".join(str(x) for x in p["primeros_3_individuos"]) if p["primeros_3_individuos"] else "—",
                ", ".join(str(x) for x in p["ultimos_3_individuos"]) if p["ultimos_3_individuos"] else "—",
                p["distancia_anterior"],
                p["distancia_siguiente"]
            ]
            filas.append(fila)
        
        return tabulate(filas, headers, tablefmt="grid", stralign="center")
        
    print("Primeros 5:\n")
    print(table(res["primeros_5"]))
    
    print("\nUltimos 5:\n")
    print(table(res["ultimos_5"]))
    pass


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    pass


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    pass


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    pass

# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 5:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
