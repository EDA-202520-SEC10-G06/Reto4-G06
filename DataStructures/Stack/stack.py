from DataStructures.List import single_linked_list as sll

def new_stack(): #size, first y last
    my_stack = sll.new_list()
    return my_stack

def push(my_stack, element):
    sll.add_last(my_stack, element)
    if type(element) == dict:
        element = element["info"]
    return my_stack

def pop(my_stack):
    last_element = sll.remove_last(my_stack)
    if last_element == None:
        raise Exception('EmptyStructureError: stack is empty')
    if type(last_element) == dict:
        last_element = last_element["info"]
    return last_element

def is_empty(my_stack):
    res = False
    stack_size = sll.size(my_stack)
    if stack_size == 0:
        res = True 
    return res

def top(my_stack):
    top_element = sll.last_element(my_stack)
    if top_element == None:
        raise Exception('EmptyStructureError: stack is empty')
    if type(top_element) == dict:
        top_element = top_element["info"]
    return top_element

def size(my_stack):
    res = sll.size(my_stack)
    return res

'''
def test():
    x = x
    return x
'''