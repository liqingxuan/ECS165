
#!/usr/bin/python 
# -*- coding: utf-8 -*-

import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")


###################------------3b------------####################
ans3bGPA = [0.0]*20 #save units*GPA
ans3bN = [0]*20 #save units

def findGPA(term):
    cursor3b1 = db.cursor()
    
    #find the units and units*GPA for each units increment
    cursor3b1.execute("SELECT COUNT(SID), su.tu, sum(Tgpa) FROM  (SELECT sum(units) AS tu, sum(units*gpa) AS Tgpa, SID FROM Transcript WHERE units <> 'NaN' AND gpa <> 'NaN' AND TERM = %s GROUP BY SID) AS su GROUP BY su.tu ORDER BY su.tu",[term])
    allGPA = cursor3b1.fetchall()
     
    #find the valid 1 to 20 range and add to the previous calculated value
    for m in range(len(allGPA)):
        if((float(allGPA[m][1])*2 )%2 ==0 and float(allGPA[m][1])>=float(1.0) and float(allGPA[m][1]) <=float(20.0)):
            ans3bN[int(allGPA[m][1])-1] = ans3bN[int(allGPA[m][1])-1] + int(allGPA[m][0])
            ans3bGPA[int(allGPA[m][1])-1] += float(allGPA[m][2])

#find all term exist in the database
cursor1a2 = db.cursor()
cursor1a2.execute("SELECT DISTINCT(TERM) FROM Course ORDER BY TERM")
allTerm = cursor1a2.fetchall()

#find the Units*GPA and units for each unit increment and for every term     
for index in range(len(allTerm)):
    findGPA(allTerm[index])

#print result
print '*****3b*****'
for i in range(1,21):
    print('Student attempt %d unit of course per quarter have an average gpa %.5f' % (i, (float(ans3bGPA[i-1]/float(ans3bN[i-1])/float(i)))))


#disconnect from server
db.close()

