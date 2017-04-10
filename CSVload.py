#ECS165A HW4 Project
#Name:Qingxuan Li   ID:913484797

import psycopg2
import csv
import os

# Open database connection
db = psycopg2.connect(database = "postgres", user=os.environ['USER'], port = "5432")

# Prepare a cursor object
cursor = db.cursor()

# Delete any existing tables with the same name of those going to build in database
deleteCourse = """Drop table if exists Course"""
deleteTranscript = """Drop table if exists Transcript"""
deleteStudent = """Drop table if exists Student"""
deleteMeetings = """Drop table if exists Meetings"""

mydata = cursor.execute(deleteCourse)
mydata = cursor.execute(deleteTranscript)
mydata = cursor.execute(deleteStudent)
mydata = cursor.execute(deleteMeetings)

# Create a course table
cursor.execute("""CREATE Table Course(
                CID varchar(60),
                TERM varchar(60),
                SUBJ varchar(20),
                CRSE varchar(60),
                SEC varchar(60),
                UNIT_MIN Float,
                UNIT_MAX Float,
                PRIMARY KEY(CID,TERM,SUBJ,CRSE)
                );""")

# Create a transcript table
cursor.execute("""CREATE Table Transcript(
                SID varchar(20),
                CID varchar(20),
                TERM varchar(20),
                SUBJ varchar(20),
                CRSE varchar(20),
                SEAT varchar(20),
                UNITS Float,
                GRADE varchar(20),
                LEVEL varchar(20),
                CLASS varchar(20),
                MAJOR varchar(20),
                STATUS varchar(20),
                GPA float,
                PRIMARY KEY(SID,CID,TERM,SUBJ,CRSE)
                )""")

# Create a student table
cursor.execute("""CREATE Table Student(
                SID varchar(64),
                SURNAME varchar(64),
                PREFNAME varchar(64),
                LEVEL varchar(20),
                CLASS varchar(20),
                PRE_ABC_MAJOR varchar(5),
                CUR_MAJOR varchar(20), 
                STATUS varchar(20),
                EMAIL varchar(64),
                PRIMARY KEY(SID)
                );""")

# Create a Meeting table
cursor.execute("""CREATE Table Meetings(
                CID varchar(20),
                TERM varchar(20),
                SUBJ varchar(20),
                CRSE varchar(20),
                SEC varchar(20),
                INST_NAME varchar(60),
                TYPE varchar(60),
                DAYS varchar(20),
                BEG_TIME varchar(20),
                END_TIME varchar(20),
                BUILD varchar(20),
                ROOM varchar(20),
                PRIMARY KEY(CID,TERM,SUBJ,CRSE,TYPE,DAYS,BEG_TIME)
                )""")


cursor.close()
print "Tables created successfully"
print "Inserting Data..."

#Create a dictionary for GPA:
GPAdict = {'A+':4, 'A':4, 'A-':3.7, 'B+':3.3, 'B':3, 'B-':2.7, 'C+':2.3, 'C':2,'C-':1.7,'D+':1.3,'D':1,'D-':0.7,'F':0}


def LoadCSVData(csv_data, quar):
    
    # Prepare a cursor object
    cursor = db.cursor()
   
    # Initialize a savepiont at the beginning 
    cursor.execute("SAVEPOINT SP")        

    num = 0 # 1:CID header  2: CID info  3: INTRUCTOR header 4:instruction info end 5:student header 6:STUDENTS info end

    CIDinfo = [None]*5 #save [CID Term Subj Crse Sec] info
    meetInfo = [None]*3
    if quar==3:
        SummerC = [None]*6 #save key:build+room value:[day, beg_time, end_time, subj, course, section]
        d = {}
    
    for row in csv_data:
        if row[0] == 'CID': #header of a new course
            num = 1
        elif num == 1: #put course info into databse
            CIDinfo = [row[0],row[1],row[2],row[3],row[4]] #update CID info for a new course
            meetInfo = [None]*3 #empty the meeting info for each new class
            
            if '-' not in row[5]: #units have a range
                cursor.execute("INSERT INTO Course(CID,TERM,SUBJ,CRSE,SEC,UNIT_MIN,UNIT_MAX) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                        [row[0],row[1],row[2],row[3],row[4],row[5],row[5]])
                    
            else: #units do not have a range
                minUnit, maxUnit  = row[5].split(" - ",1)
                cursor.execute("INSERT INTO Course(CID,TERM,SUBJ,CRSE,SEC,UNIT_MIN,UNIT_MAX) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                        [row[0],row[1],row[2],row[3],row[4],minUnit,maxUnit])
            num = 2
        elif row[0] == 'INSTRUCTOR(S)': #header of instructor line
            num = 3
        elif row[0] == 'SEAT': #header of student line
            #print"seat"
            num=5
        elif num == 3: #put instructor line info into database
            num = 3
            if(len(row)>1): # the line is not empty
                if (row[1]<>meetInfo[0] or row[2]<>meetInfo[1] or row[3]<>meetInfo[2]):
                    meetInfo = [row[1],row[2],row[3]]
                    if '-' not in row[3]: #do not have a class time range
                        #print(row)
                        cursor.execute("INSERT INTO Meetings(CID,TERM,SUBJ,CRSE,SEC,INST_NAME,TYPE,DAYS,BEG_TIME,END_TIME,BUILD,ROOM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                        [CIDinfo[0],CIDinfo[1],CIDinfo[2],CIDinfo[3],CIDinfo[4],row[0],row[1],row[2],row[3],row[3],row[4],row[5]])
                   
                    else: #have a class time range
                        bgTime, endTime = row[3].split(" - ",1)
                        if quar==3:
                            #cursor.execute("SELECT SUBJ,CRSE FROM Meetings WHERE ")
                            cursor.execute("INSERT INTO Meetings(CID,TERM,SUBJ,CRSE,SEC,INST_NAME,TYPE,DAYS,BEG_TIME,END_TIME,BUILD,ROOM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                            [CIDinfo[0],CIDinfo[1],CIDinfo[2],CIDinfo[3],CIDinfo[4],row[0],row[1],row[2],bgTime,endTime,row[4],row[5]])

                        else:        
                            cursor.execute("INSERT INTO Meetings(CID,TERM,SUBJ,CRSE,SEC,INST_NAME,TYPE,DAYS,BEG_TIME,END_TIME,BUILD,ROOM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                            [CIDinfo[0],CIDinfo[1],CIDinfo[2],CIDinfo[3],CIDinfo[4],row[0],row[1],row[2],bgTime,endTime,row[4],row[5]])
            else: #it is an empty row. end of instruction info
                num = 4
        elif (num == 5 or num == 6): #put student info into Transcript and Student table
            if(len(row)>1): # the line is not empty
                # Change the GRADE into number as GPA between A+ to F, if not inside, give it nan
                if row[8] in GPAdict:
                    GPAvalue = GPAdict[row[8]]
                else:
                    GPAvalue = float('nan')
                
                # Change units value to NaN if it is empty
                if row[5]=='':
                    Tunit = float('nan')
                else:
                    Tunit = float(row[5])
                #load the student's grade infomation into transcript
                cursor.execute("INSERT INTO Transcript(SID,CID,TERM,SUBJ,CRSE,SEAT,UNITS,GRADE,LEVEL,CLASS,MAJOR,STATUS,GPA) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                            [row[1],CIDinfo[0],CIDinfo[1],CIDinfo[2],CIDinfo[3],row[0],Tunit,row[8],row[4],row[6],row[7],row[9],GPAvalue])
                    
                
                #check student information change for different SUMMER session
               # if (quar==3): 
               #     print('summer')   
                    #student did took other class this summer session before
                    #means in the transcript same sid and term. diff level, class, major, status
                    
                #find if the SID already exist 1:exist, 0:not
                cursor.execute("SELECT COUNT(SID) FROM Student WHERE SID = %s", [row[1]])
                SIDexist = cursor.fetchone()
                
                if(SIDexist[0] == 0): #if not exist, save the student info into the database    
                    cursor.execute("INSERT INTO Student(SID, SURNAME, PREFNAME, LEVEL, CLASS, CUR_MAJOR,STATUS,EMAIL) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                            [row[1],row[2],row[3],row[4],row[6],row[7],row[9],row[10]])
                else: #Already exist in database, check major status change. Always update  SURNAME, PREFNAME, LEVEL, CLASS, CUR_Major, STATUS infomation.
                    #get the lastest student major info 
                    cursor.execute("SELECT CUR_MAJOR FROM Student WHERE SID = %s", [row[1]])
                    preMajor = cursor.fetchone()
                    
                    #if the student just transfered from another none ABC major into a ABC major, put their previous major into PRE_ABC_MAJOR
                    if(row[7][:3] == 'ABC' and preMajor[0][:3]!= 'ABC'):
                        #print "newMajor" + row[7]#[:3] #new major
                        #print "preMajor" + preMajor[0][:3] #previous major
                        #print "find major change to ABC" + row[1]
                        cursor.execute("UPDATE Student SET PRE_ABC_MAJOR=%s WHERE SID=%s",[preMajor[0],row[1]])
                    

                    #######!!!!!always update the student with the SURNAME, PREFNAME, LEVEL, CLASS, CUR_MAJOR, STATUS
                    cursor.execute("UPDATE Student SET CUR_MAJOR=%s WHERE SID=%s",[row[7],row[1]])


                num = 6;
            else:
                if(num==5): #if there is not student, rollback to the last course session. Do not save this course
                    cursor.execute("ROLLBACK TO SP")
                else: #if has student, set the savepoint here. Rollback to here if next course do not have student
                    cursor.execute("SAVEPOINT SP")        

    cursor.close()
    db.commit()
    return

csv_data = csv.reader(file('1989_Q3.csv'))
LoadCSVData(csv_data,3)
csv_data = csv.reader(file('1989_Q4.csv'))
LoadCSVData(csv_data,4)
 

#use the method mentioned from piazza @933 
for year in range(1990, 2012):
    for quarter in range(1,5):
        csv_data = csv.reader(open(str(year)+'_Q'+str(quarter)+'.csv')) 
        LoadCSVData(csv_data,quarter)

csv_data = csv.reader(file('2012_Q1.csv'))
LoadCSVData(csv_data,1)
csv_data = csv.reader(file('2012_Q2.csv'))
LoadCSVData(csv_data,2)
csv_data = csv.reader(file('2012_Q3.csv'))
LoadCSVData(csv_data,3)


# disconnect from server
db.close()

print "CSV data imported"
