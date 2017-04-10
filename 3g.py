import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------3g------------####################

#####Question 3g##### What percent of students transfer into one of the ABC majors?
cursor3g1 = db.cursor()

#find total num of student ended in ABC 
cursor3g1.execute("SELECT COUNT(SID) FROM Student WHERE CUR_MAJOR = %s", ['ABC1'])
totalCurABC1 = cursor3g1.fetchone()
cursor3g1.execute("SELECT COUNT(SID) FROM Student WHERE CUR_MAJOR = %s", ['ABC2'])
totalCurABC2 = cursor3g1.fetchone()

#find num of student transfered from other major to ABC1
cursor3g1.execute("SELECT COUNT(SID) FROM Student WHERE CUR_MAJOR = %s AND PRE_ABC_MAJOR <> %s", ['ABC1', ''])
transToABC1 = cursor3g1.fetchone()
#find num of student transfered from other major to ABC2
cursor3g1.execute("SELECT COUNT(SID) FROM Student WHERE CUR_MAJOR = %s AND PRE_ABC_MAJOR <> %s", ['ABC2', ''])
transToABC2 = cursor3g1.fetchone()

#show results
print '*****3g***** The percent of students transfer into one of the ABC majors is %.8f' % ((float(transToABC1[0]) + float(transToABC2[0]))/(float(totalCurABC1[0])+float(totalCurABC2[0])))
cursor3g1.close()


#####Question 3g##### Top 5 majors that students transfer from into ABC and their percentage
cursor3g2 = db.cursor()

#get distinct majors that transfered to ABC
cursor3g2.execute("SELECT DISTINCT PRE_ABC_MAJOR FROM Student WHERE PRE_ABC_MAJOR <> %s",[''])
preABC = cursor3g2.fetchall()
#print(preABC)
#print(len(preABC))
cursor3g2.close()

#total student transfered in to ABC
sumTrans = 0

cursor3g3 = db.cursor()
#save in an array

#for loop, while i=0, i<array.length, i++
for index in range(len(preABC)):
    #print preABC[index]
    cursor3g3.execute("SELECT COUNT(SID) FROM Student WHERE PRE_ABC_MAJOR = %s",[preABC[index]])
    cntTrans = cursor3g3.fetchone()
    sumTrans = sumTrans + cntTrans[0]
    preABC[index] = preABC[index] + cntTrans #(cursor3g3.fetchone())
    #preABC[index].append(cursor3g3.fetchone())
    #preABCnum.append(cursor3g3.fetchone())
#print(sumTrans)
#print(preABC) 
preABC = sorted(preABC, key = lambda x:x[1], reverse = True)
#print(preABC)
#select each major from list and save their counts in another lists
#sort? find maxi five number

print '*****3g***** The top 5 majors that students transfer from into ABC and their percent are:'

index = 0
for x in preABC: #and index<6:
    if(index>4):break
    index = index+1
    print '%d - Major: %s, precent: %.8f' % (index, x[0], float(x[1])/float(sumTrans))
    #print preABC[index]

cursor3g3.close()

#disconnect from server
db.close()
