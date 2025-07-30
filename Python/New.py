

# # lst = []
# # def add_movies(x):
# #     lst.append(x)
# #     return ', '.join(lst)

# # def remove_movies(x):

# #     if x in lst:
# #         lst.remove(x)
# #         return x
# #     else:
# #         print("Not there")

# # def find_movie(x):
# #     if x in lst:
# #         return str(lst.index(x))
# #     return ("Not found")

# # def reve(x):
# #     x.remove()
# #     return lst

# # print(add_movies("Inception"))
# # print(add_movies("Don3"))

# # print(remove_movies("Don3"))
# # print(remove_movies("Don2"))
# # print(lst)


# lst = [1,2,3,4]
# res = " ".join(str(x) for x in lst)
# print(res)






# emp = {}

# def add_or_up(x,y):
#     emp.update({x:y})
#     return emp

# def getSal(x):
#     return (str(emp.get(x,"not found")))

# def remove_emp(x):
#     if x in emp:
#         emp.pop(x)
#         return ("removed")
#     else:
#         return ("not found")
    
# def ListAll(x):
#     return list(emp.keys())




# emp = {}

# def addSal(em, sal):
#     emp.add({em:sal})


# def getSal(em):
#     return str(emp.get(em,"Dont work"))

# def removeEmp(em):
#     if em in emp:
#         em.pop(em)

# def list_emp():
#     return list(emp.keys())





tas = []

def schedule_tak(posi, task):
    if posi < len(tas):
        tas.insert(posi,task)
    else:
        tas.append(task)

def complete(posi):
    