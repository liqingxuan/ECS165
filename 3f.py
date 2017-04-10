

import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------3f------------####################
cursor3f1 = db.cursor()
#find all distinct major that has at least one student took ABC course
cursor3f1.execute("SELECT DISTINCT MAJOR FROM Transcript WHERE SUBJ = %s AND GPA <> %s", ['ABC', 'NaN'])
allMajor = cursor3f1.fetchall()
#print(allMajor)

cursor3f1.close()


#For each major that has student take ABC
index = 0
for index in range(len(allMajor)):

    cursor3f2 = db.cursor()

    #find GPA and units for each student took ABC in this specific major
    cursor3f2.execute("SELECT UNITS,GPA FROM Transcript WHERE SUBJ = %s AND UNITS <> %s AND GPA <> %s AND MAJOR = %s", ['ABC', 'NaN', 'NaN', allMajor[index]])
    thisMajorGPA = cursor3f2.fetchall()
    GPASum = 0  #sum of units * GPA value 
    unitsSum = 0  #sum of uni
    #calculate overall GPA over units
    for unitsGPA in thisMajorGPA:
        GPASum = GPASum + float(unitsGPA[0])*float(unitsGPA[1])
        unitsSum = unitsSum + float(unitsGPA[0])
    #print(GPASum/unitsSum)
    allMajor[index] = allMajor[index] + (float(GPASum/unitsSum),) #(cursor3g3.fetchone())

#print(allMajor)

#sort all the majors GPA in ABC course    
allMajor = sorted(allMajor, key = lambda x:x[1], reverse = True)
#print(allMajor)

best = allMajor[0][1]
worst = allMajor[len(allMajor)-1][1]
#print 'best:' + str(best)
#print 'worst:' + str(worst) 

#print(allMajor)

print '*****3f***** The major performs best on average in ABC course is/are: '
for x in allMajor:
    if(x[1] < best): break
    print x[0]



print '*****3f***** The major performs worst on average in ABC course is/are: '
#allMajor = allMajor.reverse()
#print('reverse = ')
#print(allMajor)
for x in reversed(allMajor):
    if(x[1] > worst): break
    print x[0]


#disconnect from server
db.close()
