import math as math
import random as rand
from DataStructures.List import array_list as arr
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf


def new_map(num_elements, load_factor, prime=109345121):
    capacity = int(num_elements / load_factor)
    capacity = mf.next_prime(capacity)
    
    scale = rand.randint(1,prime-1)
    shift = rand.randint(1,prime-1)
    
    table = arr.new_list()
    i = 0
    while i < capacity:
        hldr = {
            "first":None,
            "last":None,
            "size":0
        }
        arr.add_last(table, hldr)
        i += 1
    
    my_table = {}
    my_table["prime"] = prime
    my_table["capacity"] = capacity
    my_table["scale"] = scale
    my_table["shift"] = shift
    my_table["table"] = table
    my_table["current_factor"] = 0
    my_table["limit_factor"] = load_factor
    my_table["size"] = 0
    
    return my_table

def put(my_map,key,value):
    HV = mf.hash_value(my_map, key)
    Elem = my_map["table"]["elements"][HV]
    
    current = Elem["first"]

    while current != None:
        if current["key"] == key:
            current["value"] = value
            return my_map
        current = current["next"]
    
    new = {"key":key, "value": value, "next": None}
    
    if Elem["first"] == None:
        Elem["first"] = new
        Elem["last"] = new
    else:
        Elem["last"]["next"] = new
        Elem["last"] = new
        
    Elem["size"] += 1
    my_map["size"]+= 1
    
    my_map["current_factor"] = my_map["size"]/my_map["capacity"]
    
    if my_map["current_factor"]>my_map["limit_factor"]:
        my_map = rehash(my_map)
        
    return my_map

def default_compare(key, entry):

   if key == me.get_key(entry):
      return 0
   elif key > me.get_key(entry):
      return 1
   return -1
# 0 a=b         1 a>b       2 b<a

def contains(my_map, key):
    HV = mf.hash_value(my_map, key)
    Elem = my_map["table"]["elements"][HV]
    res = False
    
    current = Elem["first"]
    
    while current != None and res == False:
        if current["key"] == key:
            res = True
            return res
        current = current["next"]
    return res

def remove(my_map,key):
    HV = mf.hash_value(my_map, key)
    Elem = my_map["table"]["elements"][HV]
    
    current = Elem["first"]
    last = None
    
    while current != None:
        if current["key"] == key:
            if last == None:
                Elem["first"] = current["next"]
            #El valor era el primero
            else:
                last["next"] = current["next"]
            #El valor está en la lista
            if current == Elem["last"]:
                Elem["last"] = last
            #El valor era el último
            
            Elem["size"] -= 1
            my_map["size"] -= 1
            
            my_map["current_factor"] = my_map["size"]/my_map["capacity"]
    
            if my_map["current_factor"]>my_map["limit_factor"]:
                my_map = rehash(my_map)
            
            return current["value"]
            
        last = current
        current = current["next"]
        
    #No se encontró
    return None

def get(my_map, key):
    HV = mf.hash_value(my_map, key)
    Elem = my_map["table"]["elements"][HV]
    
    current = Elem["first"]
    while current != None:
        if current["key"] == key:
            return current["value"]
        current = current["next"]
    return None

def size(my_map):
    #my_map["size"]
    #Cuenta los elementos de cada nodo que no esté vacío
    res = 0
    elems = my_map["table"]["elements"]
    
    for i in elems:
        current = i["first"]
        
        while current != None:
            res +=1
            current = current["next"]
            
    return res

def is_empty(my_map):
    res = False
    x = size(my_map)
    if x == 0:
        res = True
    return res

def key_set(my_map):
    res = arr.new_list()
    Elems = my_map["table"]["elements"]
    
    for i in Elems:
        current = i["first"]
        while current != None:
            arr.add_last(res, current["key"])
            current = current["next"]
    
    return res
    
    
def value_set(my_map):
    res = arr.new_list()
    Elems = my_map["table"]["elements"]
    
    for i in Elems:
        current = i["first"]
        while current != None:
            arr.add_last(res, current["value"])
            current = current["next"]
    
    return res
        
def rehash(my_map):
    capacity = mf.next_prime(2*my_map["capacity"])
    
    res = new_map(capacity, my_map["limit_factor"], my_map["prime"])
    Elems = my_map["table"]
    
    for i in Elems:
        current = i["first"]
        while current != None:
            put(res,current["key"], current["value"])
            current = current["next"]
    
    return res
"""
x = new_map(5,1,109345121)
print(x)
"""