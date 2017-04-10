
#!/usr/bin/python 
# -*- coding: utf-8 -*-

import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------3a------------####################
ans3a = [0.0]*20

def findNum(term):
    cursor1a1 = db.cursor()
    cursor1a1.execute("SELECT COUNT(SID), su.tu FROM  (SELECT sum(units) AS tu, SID FROM Transcript WHERE units <> 'NaN' AND TERM = %s GROUP BY SID) AS su GROUP BY su.tu ORDER BY su.tu",[term])
    allStu = cursor1a1.fetchall()

    totalNum = 0.0 
        
    for k in range(len(allStu)):
        if(float(allStu[k][1])>=float(1.0) and float(allStu[k][1]) <=float(20.0)):
            totalNum = totalNum + allStu[k][0]
    
    for m in range(len(allStu)):
        if((float(allStu[m][1])*2 )%2 ==0 and float(allStu[m][1])>=float(1.0) and float(allStu[m][1]) <=float(20.0)):
            ans3a[int(allStu[m][1])-1] = ans3a[int(allStu[m][1])-1] + float(allStu[m][0]) 

    cursor1a1.close()

    return totalNum

#find all term exist in the database
cursor1a2 = db.cursor()
cursor1a2.execute("SELECT DISTINCT(TERM) FROM Course ORDER BY TERM")
allTerm = cursor1a2.fetchall()

#calculate the precent of student for each unit increment and for every term
num = 0
for index in range(len(allTerm)):
    j = findNum(allTerm[index])
    num = num + j

#print result
print '*****3a*****'
for i in range(1,21):
    print('Student attempt %d unit of course per quarter is %.5f' % (i, float(ans3a[i-1])/float(num)))


#disconnect from server
db.close()

