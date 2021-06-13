import csv
import pandas
import pandas as pd


class carbon_nano():
    pass
    def carbon_file(self):



        list1 = []
        list2 = []
        list3 = []

        with open('carbon_nanotubes.csv', 'r') as in_file:
            with open('new_file2.csv', 'w') as out_file:
                data = csv.reader(in_file,delimiter=';')
                header = next(data)

    #    print(header)
                for i in data:

                    list1.append(i)
            #        print(list1)

                writer = csv.writer(out_file)
                writer.writerow(header)
            #    print(list1)
                writer.writerows(list1)
    #    print(list1)





        with open('new_file2.csv', newline='') as csvfile:
            with open('new_file3.csv', 'w') as out1_file:
                 data = csv.DictReader(csvfile)

                 for row in data:
                     row1 = (row['Chiral indice n'], row['Chiral indice m'],row['Initial atomic coordinate u'].replace(',','.'))

                     list2.append(row1)
                 list3.append(['Chiral_indice_n', 'Chiral_indice_m', 'Initial_atomic_coordinate_u'])
                 writer = csv.writer(out1_file)
                 writer.writerows(list3)
                 writer.writerows(list2)
        data1 = pd.read_csv("new_file3.csv")
        df = data1.dropna(axis=0,how='any')
        df.to_csv('new_file4.csv',index=False)


