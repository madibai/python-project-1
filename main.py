import psycopg2
import csv
import xml.etree.ElementTree as ET

def task1():
    f_in = open('1/input.txt')
    f_lines = f_in.readlines()
    f_out = open('1/output.txt','w+')
    for i in f_lines:
        #print(len(max(i.split('1'), key=len)))
        f_out.write('%d\n' % len(max(i.split('1'), key=len)))
    f_out.close()

def task2():
    f_in = open('2/input.txt')
    f_lines = f_in.readlines()
    f_out = open('2/output.txt', 'w+')
    sub1 = '<--<<'
    sub2 = '>>-->'
    results = 0
    sub_len1 = len(sub1)
    sub_len2 = len(sub2)

    for i in f_lines:
        for j in range(len(i)):
            if i[j:j + sub_len1] == sub1:
                results += 1
            elif i[j:j + sub_len2] == sub2:
                results += 1
        #print(results)
        f_out.write('%d\n' % results)
    f_out.close()

def task3():
    f_in = open('3/input.txt')
    f_lines = f_in.readlines()
    f_out = open('3/output.txt', 'w+')
    counter = 1
    for i in f_lines:
        sum = 0
        if counter%2 != 1:
            results = list(map(int, i.split(' ')))
            ind_min = results.index(min(results))
            ind_max = results.index(max(results))
            if ind_min>ind_max:
                ind_min, ind_max = ind_max, ind_min
            multy = 1
            for j in results[ind_min+1:ind_max]:
                multy *= j
            for j in results:
                if j > 0:
                    sum += j
            #print(sum, multy)
            f_out.write('%d %d\n' % (sum, multy))
        counter += 1
    f_out.close()



def task4():
    f_in = open('4/input.txt')
    f_lines = f_in.readlines()
    f_out = open('4/output.txt', 'w+')
    counter = 1
    n = 0
    for i in f_lines:
        current_index = -1
        sum = 0
        ost = n
        nakoplennye_dni = 0
        if counter % 2 == 1:
            n = int(i.strip())
        else:
            results = list(map(int, i.split(' ')))
            dictResults = {k: v for v, k in enumerate(results)}
            results.sort(reverse=True)
            for j in results:
                if ost <= 0:
                    break
                if (dictResults.get(j) + 1) < current_index:
                    continue
                current_index = max(current_index, (dictResults.get(j) + 1))
                dni = (dictResults.get(j) + 1) - nakoplennye_dni
                sum += min(dni,ost) * j
                ost -= min(dni,ost)
                nakoplennye_dni += dni
            #print(sum)
            f_out.write('%d\n' % (sum))
        counter += 1
    f_out.close()

def task5():
    f_in = open('5/input.txt')
    f_lines = f_in.readlines()
    f_out = open('5/output.txt', 'w+')
    for i in f_lines:
        if len(set(i.strip())) <= 2:
            f_out.write('%d\n' % int(i.strip()))
            continue
        else:
            digit = list(map(int,i.strip()))
            #print(digit)
            j = len(digit)
            min = 0
            while j > 0:
                digit[j-1]
                j -= 1
        #f_out.write('%d\n' % (sum))
    f_out.close()

def task6_rec(input):
    def recurs(st1):
        yield st1
        while len(st1) > 1:
            st1 = st1[:-2] + [st1[-2] + st1[-1]]
            #print('1st')
            for prefix in recurs(st1[:-1]):
                if not prefix or prefix[-1] <= st1[-1]:
                    yield prefix + [st1[-1]]

    return list(recurs([1] * input))


def task6():
    f_in = open('6/input.txt')
    f_lines = f_in.readlines()
    f_out = open('6/output.txt', 'w+')
    for i in f_lines:
        answer = task6_rec(int(i.strip()))
        #(lambda x: str(x) + '+' + str(x))(x) for x in j
        for j in answer:
            f_out.write('%s\n' % (j))
            #a = [(lambda x: str(x) + '+')(x) for x in j]
            #print(a)
    f_out.close()

def easy_TwoSum1(nums, target):
    a = [2,7,11,15]
    a.sort()
    arr = []
    for i in range(len(nums)):
        if i > 0 and nums[i] == nums[i - 1]:   continue
        s2 = i + 1
        while len(arr)==0:
            sums = nums[i]+nums[s2]
            if sums == target:
                arr.append([nums[i], nums[s2]])
                a1 = i
                a2 = s2
                break
            if sums < target:
                s2 += 1
            else:
                i -= 1
    print([a1, a2])
    print(target)

def easy_TwoSum2(nums, target):
    complement = {}
    for i, v in enumerate(nums):
        # if v = nums[i] in complement, we find a solution
        if v in complement:
            return complement[v], i
        # if v = nums[i] not in complement, we store the element into the dictionary.
        else:
            complement[target - v] = i
    return -1

def easyReverseInteger(x):
    temp = (x > 0) - (x < 0)
    rv = int(str(x * temp)[::-1])
    return temp * rv * (rv < 2 ** 31)


def beeline():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="p@ssw0rd",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="beeline")

        cursor = connection.cursor()

        s,names_string, players_data = new_parse()
        cursor.execute(s)
        connection.commit()
        dd = fill_table_player(players_data)
        for i in dd:
            cursor.execute('INSERT INTO player VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                           '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',i.split(','))
            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")

def new_parse():
    with open("data.csv", "r",encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=",")
        a = []
        for i, line in enumerate(reader):
            a.append(line)
    s, names_string = table_create_command(a[0])
    return s, names_string, a[1:]

def table_create_command(a):
    s = 'CREATE TABLE IF NOT EXISTS player ( ID SERIAL PRIMARY KEY, '
    column_names = a[2:]
    names_string = ''
    for j in column_names:
        s = s + str(j).replace(' ', '_') + ' VARCHAR(255), '
        names_string = names_string + str(j).replace(' ', '_') + ', '
    s = s[:-2] + ')'
    names_string = names_string [:-2]
    return s,names_string

def fill_table_player(a):
    players_data = a
    data = []
    for i in players_data:
        sj = ''
        for j in i[1:]:
            sj = sj + str(j).strip().replace(',','.')+', '
        data.append(sj[:-2])
    return data




def get_parse1c():
    f_in = open('C:\F\data.xml',encoding='utf-8')
    f_lines = f_in.readlines()
    a = []
    need_read = 0;
    s = ''
    for i in f_lines:
        if (i.find('<v8e:Date>')>0 or i.find('<v8e:EventPresentation>')>0 or i.find('<v8e:UserName>')>0 or \
                i.find('<v8e:MetadataPresentation>')>0 or i.find('<v8e:Data xmlns')>0) and need_read == 1:
            ii = i.strip()
            s = s + ', ' + ii[ii.find('<v8e:') + 5:ii.find('>')] + '=' + ii[ii.find('>') + 1:ii.find('</')]
        if i.strip() == '<v8e:Event>':
            need_read = 1
        elif i.strip() == '</v8e:Event>':
            need_read = 0
            a.append(s)
            s = ''
    #print(len(a))
    return a

def fill_table_1c(data):
    a = []
    for i in data:
        if i.find('Date')>0 or i.find('EventPresentation')>0 or i.find('UserName')>0 or \
                i.find('MetadataPresentation')>0 or i.find('Data xmlns')>0:
            s = ''
            for j in i.split(',')[1:]:
                try:
                    s = s + j.split('=')[1] + '||'
                except (Exception, psycopg2.Error) as error: \
                    print(error, j)
            a.append(s[:-2])
        else:
            continue

        #if i.find('Level=Error')>0 or i.find('BackgroundJob')>0 or i.find('_$Session$_')>0 or i.find('_$Access$_')>0 or i.find('_$User$_.New')>0 or i.find('_$Transaction$_')>0:
        #    continue
        #else:
        #    s = ''
        #    for j in i.split(',')[1:]:
        #        try:
        #            s = s + j.split('=')[1]+'||'
        #        except (Exception, psycopg2.Error) as error:\
        #            print(error,j)
        #
        #   a.append(s[:-2])
    return a




def create_table_for_1c(a):
    s = 'CREATE TABLE IF NOT EXISTS logs_1c ( '
    #column_names = a
    #names_string = ''
    #for j in column_names.split(',')[1:]:
    #    s = s + j[:j.find('=')].strip().replace('/','') + ' VARCHAR(255), '
    #    #s = s + str(j).replace(' ', '_') + ' VARCHAR(255), '
    #    names_string = names_string + j[:j.find('=')].strip().replace('/','') + ', '
    #s = s.replace('User ','UserId ')
    #s = s.replace('Data xsi:nil','data_xsi ')
    #s = s[:-2] + ')'
    #names_string = names_string.replace('User,', 'UserId,')
    #names_string = names_string.replace('Data xsi:nil', 'data_xsi')
    #names_string = names_string[:-2]

    s = s + 'Date VARCHAR(255), EventPresentation VARCHAR(255), UserName VARCHAR(255), MetadataPresentation VARCHAR(255), Data VARCHAR(255))'
    #names_string = 'Date, EventPresentation, UserName, MetadataPresentation, Data'
    return s

def fill_parse_table(data):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="p@ssw0rd",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="beeline")

        cursor = connection.cursor()

        s = create_table_for_1c(data[0])

        cursor.execute(s)
        connection.commit()
        dd = fill_table_1c(data)
        for i in dd:
            cursor.execute('INSERT INTO logs_1c VALUES (%s,%s,%s,%s,%s)',i.split('||'))
            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")



if __name__ == '__main__':
    #task1()
    #task2()
    #task3()
    #task4()
    #task5()
    #task6()
    #easy_TwoSum1([2,7,11,15],9)
    #print(easy_TwoSum2([2, 7, 11, 15], 17))
    #print(easyReverseInteger(12345))
    data = get_parse1c()
    fill_parse_table(data)

    #beeline()
    #parseCSV()
    #new_parse()
    #fill_table_player()
