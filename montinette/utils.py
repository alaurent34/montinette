import numpy as np

def _to_hour_bin(a):
    """   Take a (5, n) array representing minutes duration transaction data. 
    Columns order is : ['dur (s)', 'min_sta','min_end', 'h_sta', 'h_end', 'day']. 
    """

    #create new temp cupy array b to contain minute duration per hour.  
    b = np.zeros((len(a),24))
    for j in range(0,len(a)):
        hours = int((a[j][0]/3600)+(a[j][1]/60))
        if(hours==0): # within same hour
            b[j][a[j][3]] = int(a[j][0]/60)
        elif(hours==1): #you could probably delete this condition.
            b[j][a[j][3]] = 60-a[j][1]
            b[j][a[j][4]] = a[j][2]
        else:
            b[j][a[j][3]] = 60-a[j][1]
            if(hours<24): #all array elements will be all 60 minutes if durationa is over 24 hours
                if(a[j][3]+hours<24):
                    b[j][a[j][3]+1:a[j][3]+hours]=60
                    b[j][a[j][4]] = a[j][2]
                else:
                    b[j][a[j][3]+1:24]=60
                    b[j][0:(a[j][3]+1+hours)%24]=60
                    b[j][a[j][4]] = a[j][2]
                    
    return b