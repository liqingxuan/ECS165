import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------3d------------####################
#Question 3d: for each ABC100 level course, find the easiest and hardest prof, provide name and avg gpa

#For those class that have a grade:
cursor3d1 = db.cursor()
cursor3d1.execute("SELECT t.CRSE, m.inst_name, sum(t.units), sum(t.units * t.gpa) FROM meetings m, transcript t WHERE m.CID = t.CID AND m.term = t.term AND m.inst_name <> '' AND t.units <> 'NaN' AND t.GPA <> 'NaN' AND t.SUBJ = 'ABC' AND t.CRSE < '200' AND t.CRSE >= '100' GROUP BY t.SUBJ, t.CRSE, m.inst_name ORDER BY t.SUBJ, t.CRSE")

crseInst = cursor3d1.fetchall()

insHName = crseInst[0][1]
insH = 4.0

insEName = crseInst[0][1]
insE = 0.0

crse = crseInst[0][0]

print '*****3d*****'

for i in range(len(crseInst)):
    if(crseInst[i][0]==crse):
        if(float(crseInst[i][3])/float(crseInst[i][2]) > insE):
            insEName = crseInst[i][1]
            insE = float(crseInst[i][3]) / float(crseInst[i][2])
        if(float(crseInst[i][3])/float(crseInst[i][2]) < insH):
            insHName = crseInst[i][1]
            insH = float(crseInst[i][3]) / float(crseInst[i][2])
          
    else:
        print('The easiest instructor for ABC %s is %s His average grade is %s' % (crse, insEName, insE))
        print('The hardest instructor for ABC %s is %s His average grade is %s' % (crse, insHName, insH))
        crse = crseInst[i][0]
        insHName = crseInst[i][1]
        insH = float(crseInst[i][3]) / float(crseInst[i][2])
        insEName = crseInst[i][1]
        insE = float(crseInst[i][3]) / float(crseInst[i][2])

    
    
print('The easiest instructor for ABC %s is %s His average grade is %s' % (crse, insEName, insE))
print('The hardest instructor for ABC %s is %s His average grade is %s' % (crse, insHName, insH))
    
cursor3d1.close()


# for those class only have a p/np grade

cursor3d2 = db.cursor()
cursor3d3 = db.cursor()

cursor3d2.execute("SELECT t.CRSE, m.inst_name, COUNT(t.GRADE),COUNT(t.SID)  FROM meetings m, transcript t WHERE m.CID = t.CID AND m.term = t.term AND m.inst_name <> '' AND t.units <> 'NaN' AND t.GRADE = 'P' AND t.SUBJ = 'ABC' AND t.CRSE < '200' AND t.CRSE >= '100' GROUP BY t.SUBJ, t.CRSE, m.inst_name ORDER BY t.SUBJ, t.CRSE")

p = cursor3d2.fetchall()

crse = p[0][0]

easy = 0.0
hard = 4.0

for i in range(len(p)):
    #print(p[i])
    cursor3d3.execute("SELECT COUNT(GPA) FROM transcript t WHERE t.SUBJ = 'ABC' AND t.CRSE = %s AND GPA <> 'NaN'", [p[i][0]])
    hasGPA = cursor3d3.fetchone()
    #print(hasGPA)
    if(hasGPA[0] == 0):
        cursor3d4 = db.cursor()
        cursor3d4.execute("SELECT COUNT(t.SID)FROM meetings m, transcript t WHERE m.CID = t.CID AND m.term = t.term AND m.inst_name = %s AND t.CRSE = %s", [p[i][1], p[i][0]])
        stuNum = cursor3d4.fetchone()
        #print(stuNum) 

        #Hinst = []
        #Einst = []
        if(p[i][0] == crse):
            if(float(p[i][3])/float(stuNum[0]) > easy):
                Einst = []
                easy =float(p[i][3])/float(stuNum[0])
                Einst.append(p[i][1])
            elif(float(p[i][3])/float(stuNum[0]) == easy):
                Einst.append(p[i][1])
            if(float(p[i][3])/float(stuNum[0]) < hard):
                Hinst = []
                hard =float(p[i][3])/float(stuNum[0])
                Hinst.append(p[i][1])
            elif(float(p[i][3])/float(stuNum[0]) == hard):
                Hinst.append(p[i][1])
        else:
            print('The easiest instructor for ABC %s is %s His average pass rate is %s' % (crse, Einst, easy))
            print('The hardest instructor for ABC %s is %s His average pass rate is %s' % (crse, Hinst, hard))
            Einst = []
            Hinst = []
            Einst.append(p[i][1])
            Hinst.append(p[i][1])
            easy = float(p[i][3])/float(stuNum[0])
            hard = float(p[i][3])/float(stuNum[0])
            crse = p[i][0]

print('The easiest instructor for ABC %s is %s His average pass rate is %s' % (crse, Einst, easy))
print('The hardest instructor for ABC %s is %s His average pass rate is %s' % (crse, Hinst, hard))

cursor3d2.close()


#disconnect from server
db.close()
