import psycopg2


#open databse connection
db = psycopg2.connect(database = "postgres")

###################------------5a------------####################

print '*****5a***** The prerequisites for ABC 203, ABC 210 and ABC 222 are:'

def preRequest(subj, crse):
    cursor5a1 = db.cursor()

    #total number of students those have taken ABC 203
    cursor5a1.execute("SELECT COUNT(DISTINCT(SID)) FROM Transcript WHERE SUBJ = %s AND CRSE = %s",[subj, crse])
    ABC203total = cursor5a1.fetchone()
    cursor5a1.close()

    #calculate for each percentage 
    cursor5a2 = db.cursor()
    x = 1.0
    while(x>0.7):
        cursor5a2.execute(" SELECT oc.SUBJ, oc.CRSE FROM (SELECT SID,TERM,SUBJ,CRSE FROM Transcript) oc  CROSS JOIN (SELECT SID,TERM,SUBJ,CRSE FROM Transcript WHERE (SUBJ = %s AND CRSE = %s)) nc WHERE (nc.SID = oc.SID  AND oc.TERM < nc.TERM) GROUP BY (oc.SUBJ, oc.CRSE) HAVING COUNT(DISTINCT(oc.SID)) >= %s", [
subj, crse, x * float(ABC203total[0]) ])

        ABC203 = cursor5a2.fetchall()
        #print(ABC203)
        print 'courses %s percent student have taken prior to %s %s:'  % (str(x*100), str(subj), str(crse))
        if(len(ABC203)==0): print('None.')
        for index in range(len(ABC203)):
            print (ABC203[index][0] + ABC203[index][1])

        x = x-0.05
    cursor5a2.close()


preRequest('ABC','203')
preRequest('ABC','210')
preRequest('ABC','222')






#disconnect from server
