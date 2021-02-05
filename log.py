import os
import pickle
import table
import database
class Log:


    def save_table_data(db_object,table_name):
        #Save a copy of the table before changes so that rollback is possible
        file_path = 'tableforms.data'
        tables=[]
        # check if size of file is 0
        if os.stat(file_path).st_size > 0:
            with open('tableforms.data', 'rb') as filehandle:
                # read the data as binary data stream
                tables= pickle.load(filehandle)
        tables.append(db_object.tables[table_name])
        with open('tableforms.data', 'wb') as filehandle:
            # store the data as binary data stream
            pickle.dump(tables, filehandle)
        #Save the insert_stack of the table
        file_path2 = 'stack.data'
        stack=[]
        # check if size of file is 0
        if os.stat(file_path2).st_size > 0:
            with open('stack.data', 'rb') as filehandle:
                # read the data as binary data stream
                stack= pickle.load(filehandle)
        stack.append(db_object._get_insert_stack_for_table(table_name))
        with open('stack.data', 'wb') as filehandle:
            # store the data as binary data stream
            pickle.dump(stack, filehandle)
        #Record to logger the database name, the table name and the index name of the table if it exists
        f = open('logger.txt','a')
        f.write(str(db_object._name)+","+str(table_name))
        f.write("\n")
        f.close()
        
    def rollback(self,N,database_name=None,table_name=None):
        print('Starting rollback...')
        #Save the contents of the logger to a list
        # define empty list
        places = []
        # open file and read the content in a list
        with open('logger.txt', 'r') as filehandle:
            filecontents = filehandle.readlines()

            for line in filecontents:
                # remove linebreak which is the last character of the string
                current_place = line[:-1]

                # add item to the list
                places.append(current_place)
        #Reverse the list so that the last actions in the database will be rollbacked first
        places.reverse()
        if len(places)<N:
            print('The rollback is not possible')
            return
        if database_name!=None and table_name!=None:
            for i in range(N):
                if places[i].count(database_name)>0 and places[i].count(table_name)>0:
                    if places[i].count('table_creation')>0 or places[i].count('table_creation_from_csv')>0:
                        db = database.Database(database_name)
                        db.drop_table(table_name)
                    else:
                        file_path = 'tableforms.data'
                        file_path2 = 'stack.data'
                        tables=[]
                        stack=[]
                        if os.stat(file_path).st_size > 0 and os.stat(file_path2).st_size > 0:
                            components = places[i].split(",")
                            with open('tableforms.data', 'rb') as filehandle:
                                # read the data as binary data stream
                                tables= pickle.load(filehandle)
                            pinakas=tables[len(tables)-1]
                            tables.pop(len(tables)-1)
                            with open('tableforms.data', 'wb') as filehandle:
                                # store the data as binary data stream
                                pickle.dump(tables, filehandle)
                            with open('stack.data', 'rb') as filehandle:
                                # read the data as binary data stream
                                stack= pickle.load(filehandle)
                            metabliti_gia_to_stack=stack[len(stack)-1]
                            stack.pop(len(stack)-1)
                            with open('stack.data', 'wb') as filehandle:
                                # store the data as binary data stream
                                pickle.dump(stack, filehandle)
                            db = database.Database(database_name)
                            db.drop_table(table_name,False)
                            db.table_from_object(pinakas)
                            db._update_meta_insert_stack_for_tb(table_name,metabliti_gia_to_stack)
                            db._update()
                        else:
                            print('NO COMMANDS EXECUTED YET')
                            print('End of rollback.')
                            return
            print('End of rollback.')
            for i in range(N):
                places.pop(i)
            places.reverse()
            with open('logger.txt', 'w') as filehandle:
                filehandle.writelines("%s\n" % place for place in places)
        elif database_name!=None:
            for i in range(N):
                if places[i].count(database_name)>0:
                    if places[i].count('table_creation')>0 or places[i].count('table_creation_from_csv')>0:
                        db = database.Database(database_name)
                        db.drop_table(table_name)
                    elif places[i].count('database_drop')>0:
                        print('We can\'t rollback a dropped database!')
                        print('End of rollback.')
                        return
                    elif places[i].count('database_creation')>0:
                        db = database.Database(database_name)
                        db.drop_db()
                    else:
                        file_path = 'tableforms.data'
                        file_path2 = 'stack.data'
                        tables=[]
                        stack=[]
                        if os.stat(file_path).st_size > 0 and os.stat(file_path2).st_size > 0:
                            components = places[i].split(",")
                            with open('tableforms.data', 'rb') as filehandle:
                                # read the data as binary data stream
                                tables= pickle.load(filehandle)
                            pinakas=tables[len(tables)-1]
                            tables.pop(len(tables)-1)
                            with open('tableforms.data', 'wb') as filehandle:
                                # store the data as binary data stream
                                pickle.dump(tables, filehandle)
                            with open('stack.data', 'rb') as filehandle:
                                # read the data as binary data stream
                                stack= pickle.load(filehandle)
                            metabliti_gia_to_stack=stack[len(stack)-1]
                            stack.pop(len(stack)-1)
                            with open('stack.data', 'wb') as filehandle:
                                # store the data as binary data stream
                                pickle.dump(stack, filehandle)
                            db = database.Database(database_name)
                            db.drop_table(components[1],False)
                            db.table_from_object(pinakas)
                            db._update_meta_insert_stack_for_tb(table_name,metabliti_gia_to_stack)
                            db._update()
                        else:
                            print('NO COMMANDS EXECUTED YET')
                            return
            print('End of rollback.')
            for i in range(N):
                places.pop(i)
            places.reverse()
            with open('logger.txt', 'w') as filehandle:
                filehandle.writelines("%s\n" % place for place in places)
        else:
            print('Rollback can only be perfomed for a database or a table of a database')
                    
                
    def trlog(record):
        f = open("Analitikolog.txt", "a")
        f.write(str(record))
        f.write("\n")
        f.close()

    def show_log(self):
        places = []
        # open file and read the content in a list
        with open('Analitikolog.txt', 'r') as filehandle:
            filecontents = filehandle.readlines()

            for line in filecontents:
                # remove linebreak which is the last character of the string
                current_place = line[:-1]

                # add item to the list
                places.append(current_place)
        for i in range(len(places)):
            print(places[i])
                
