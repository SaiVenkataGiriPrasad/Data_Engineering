import random 
import time
names = ['SAI', 'UDAY', 'BHARATH', 'RISHIKA', 'LAHARI', 'PRAVALLIKA', 'RAMYA', 'VIDYA', 'SOWRYA', 'NIKHIL', 'MANI', 'JACK', 'HIRAL', 'TOM CRUISE', 'VISHNU', 'VENKAT', 'RACHANA', 'PRUTHVI', 'GIREESH', 'LASYA', 'SOWMINI']


def create_dataset():
    """ This function is used to create the dataset of 5000000 rows
      with the names that have listed"""
    file_c = open('generate_dataset.txt', 'w', encoding = 'UTF-8')
    create_rows = 5000000 # Number of rows creating in a file
    for _ in range(create_rows):
        generate_data = random.choice(names)
        file_c.write(generate_data + '\n')
    file_c.close()


def read_dataset_list():
    ''' This funtion is defined to read the dataset and to count the number of repetitions of the names'''
    names_count_list = []
    for name in names: #append the list with zero, referencing with indexes of names.
        names_count_list.append(0)
    with open('generate_dataset.txt', 'r', encoding='UTF-8') as file_r:
        for name in file_r:
            name = name.strip()
            if name != '':
                names_count_list[names.index(name)] += 1
    print(names_count_list)


def read_dataset_dict():
    ''' This function is defined to read the dataset and to count the number of repetitions of the names'''
    names_count_dict = {}
    for name in names:
        names_count_dict[name] = 0 # Mapping value(zero) with key(name)
    with open('generate_dataset.txt', 'r', encoding = 'UTF-8') as file_r:
        for name in file_r:
            name = name.strip()
            if name != '':
                names_count_dict[name] += 1
    print(names_count_dict)

# By comparing the two methods(list & dict), you'll be able to see dictionary is significantly faster than the list.
# Time taking for list objects
create_dataset()

t0 = time.time()
read_dataset_list()
t1 = time.time()
print("TIME TAKEN FOR THE LIST {}".format(t1-t0))

# Time taking for dict objects 
t2 = time.time()
read_dataset_dict()
t3 = time.time()
print("TIME TAKEN FOR THE DICT {}".format(t2-t3))
