
import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------3c------------####################
#Question 3c:Name and average grade of easiet and hardest instructor
cursor3c = db.cursor()

#find all instructors ans his assigned total GPA and total GPA*units
cursor3c.execute("SELECT m.inst_name, sum(t.units), sum(t.units * t.gpa) AS UG FROM meetings m, transcript t WHERE m.CID = t.CID AND m.term = t.term AND m.inst_name <> 'NaN' AND t.units <> 'NaN' AND t.GPA <> 'NaN' GROUP BY m.inst_name ")

hardest = cursor3c.fetchall()

#pre assigned the value
easyInst = 0.0
hardInst = 4.0
easyName = hardest[0][0]
hardName = hardest[0][0]

#compare those values to find the hardest and the easiest
for i in range(len(hardest)):
    if(float(hardest[i][2])/float(hardest[i][1]) > easyInst):
        easyInst = float(hardest[i][2])/float(hardest[i][1])
        easyName = hardest[i][0]
    if(float(hardest[i][2])/float(hardest[i][1]) < hardInst):
        hardInst = float(hardest[i][2])/float(hardest[i][1])
        hardName = hardest[i][0]

print '*****3c*****'
print('The easiest instructor is %s His average grade is %s'% (easyName, str(easyInst)))
print('The hardest instructor is %s His average grade is %s'% (hardName, str(hardInst)))

#disconnect from server
db.close()


