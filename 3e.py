import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------3e------------####################

##find summer session conflict based on student information:
cursor3e1 = db.cursor()

#select subj and crse that have same SID and term in Transcript that has different level or class or major or status.
cursor3e1.execute(""" (SELECT t1.SUBJ,t1.CRSE,t2.SUBJ,t2.CRSE 
    FROM (SELECT SID,TERM,SUBJ,CRSE,LEVEL FROM Transcript) t1 
    CROSS JOIN 
    (SELECT SID,TERM,SUBJ,CRSE,LEVEL FROM Transcript) t2 
    WHERE (t1.SID = t2.SID AND t1.TERM = t2.TERM AND t1.LEVEL <> t2.LEVEL)) UNION 
    (SELECT t1.SUBJ,t1.CRSE,t2.SUBJ,t2.CRSE 
        FROM (SELECT SID,TERM,SUBJ,CRSE,CLASS FROM Transcript) t1 
        CROSS JOIN 
        (SELECT SID,TERM,SUBJ,CRSE,CLASS FROM Transcript) t2 
        WHERE (t1.SID = t2.SID AND t1.TERM = t2.TERM AND t1.CLASS <> t2.CLASS)) UNION 
            (SELECT t1.SUBJ,t1.CRSE,t2.SUBJ,t2.CRSE 
                FROM (SELECT SID,TERM,SUBJ,CRSE,MAJOR FROM Transcript) t1 
                CROSS JOIN 
                (SELECT SID,TERM,SUBJ,CRSE,MAJOR FROM Transcript) t2 
                WHERE (t1.SID = t2.SID AND t1.TERM = t2.TERM AND t1.MAJOR <> t2.MAJOR)) UNION 
                    (SELECT t1.SUBJ,t1.CRSE,t2.SUBJ,t2.CRSE 
                        FROM (SELECT SID,TERM,SUBJ,CRSE,STATUS FROM Transcript) t1 
                        CROSS JOIN 
                        (SELECT SID,TERM,SUBJ,CRSE,STATUS FROM Transcript) t2 
                        WHERE (t1.SID = t2.SID AND t1.TERM = t2.TERM AND t1.STATUS <> t2.STATUS)) """)

studentDiff = cursor3e1.fetchall()
mergedS = []

#print '*****3e***** Merged summer sessions are: '
for index in range(len(studentDiff)):
    if (studentDiff[index][0] < studentDiff[index][2]) or (studentDiff[index][0] == studentDiff[index][2] and studentDiff[index][1] < studentDiff[index][3]) :
        #print studentDiff[index] 
        mergedS.append(studentDiff[index])
cursor3e1.close()

#print(mergedS)
##find summer session conflict based on course information
cursor3e2 = db.cursor()

cursor3e2.execute("(SELECT m1.CRSE,m1.SUBJ,m1.DAYS,m1.END_TIME,m2.CRSE,m2.SUBJ,m2.DAYS,m2.BEG_TIME,m1.TERM,m1.BEG_TIME,m2.END_TIME FROM (SELECT TERM,CRSE,SUBJ,DAYS,BEG_TIME,END_TIME,BUILD,ROOM FROM Meetings) m1 CROSS JOIN (SELECT TERM,CRSE,SUBJ,DAYS,BEG_TIME,END_TIME,BUILD,ROOM FROM Meetings) m2 WHERE (m1.TERM = m2.TERM AND m1.BUILD = m2.BUILD AND m1.ROOM = m2.ROOM AND m1.DAYS<> %s AND m1.BEG_TIME<> %s AND m1.END_TIME<> %s AND m2.DAYS<> %s AND m2.BEG_TIME<> %s AND m2.END_TIME<> %s AND (m1.CRSE <> m2.CRSE OR m1.SUBJ <>m2.SUBJ)))", ['', '', '', '', '',''])

meetDiff = cursor3e2.fetchall()
#print(meetDiff)


for index in range(len(meetDiff)):
    if (int(meetDiff[index][8])%10 == 6 and meetDiff[index][3][-2:] <= meetDiff[index][7][-2:]):
        hr1, mins1 = meetDiff[index][3].split(":",1) #m1endtime
        hr2, mins2 = meetDiff[index][7].split(":",1) #m2begtime

        hr3, mins3 = meetDiff[index][9].split(":",1) #m1begtime
        hr4, mins4 = meetDiff[index][10].split(":",1)  #m2endtime

        hr1 = int(hr1)
        hr2 = int(hr2)
        hr3 = int(hr3)
        hr4 = int(hr4)
        mins1 = int(mins1[:2])
        mins2 = int(mins2[:2])
        mins3 = int(mins3[:2])
        mins4 = int(mins4[:2])

        #change to 24hr time
        if(meetDiff[index][3][-2:] == 'PM' and hr1<>12):
            hr1 = hr1 + 12
        if(meetDiff[index][7][-2:] == 'PM' and hr2<>12):
            hr2 = hr2 + 12
        if(meetDiff[index][9][-2:] == 'PM' and hr3<>12):
            hr3 = hr3 + 12
        if(meetDiff[index][10][-2:] == 'PM' and hr4<>12):
            hr4 = hr4 + 12

        #if the second start time is earlier than the first end time, and also the second end time is not earlier than the first start time. Conflict!
        if(hr1 > hr2 or (hr1 == hr2 and mins1 > mins2)):
            if(hr4 > hr3 or (hr3==hr4 and mins4 > mins3)):
                if(len(set(meetDiff[index][2]).intersection(meetDiff[index][6]))>0):
                    if (meetDiff[index][1] < meetDiff[index][5]) or (meetDiff[index][1] == meetDiff[index][5] and meetDiff[index][0] < meetDiff[index][4]):
                        t = ()
                        t = t + (meetDiff[index][1], meetDiff[index][0], meetDiff[index][5], meetDiff[index][4])

                        mergedS.append(t)

print '*****3e***** Merged summer sessions are: '
for index in set(mergedS):
    print(index)

cursor3e1.close()

#disconnect from server
db.close()
