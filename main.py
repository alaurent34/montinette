import numpy as np
import pandas as pd

from montinette.sensors import CounterDataFrame

def main():
    
    # Data from all sensors
    df = pd.read_csv('./input/capteurs/Sensors.csv', sep=';')
    
    # Divide data by sensors type
    ## Sensors for the vehicules which went in the parking lot
    cdf_in_1 = CounterDataFrame(
        df[df['Entity Name'] == '034050780000000A'].copy(),
        datetime='Timestamp',
        counter='eventsCountA',
        name='Entity Name',
        timestamp=True
    )
    cdf_in_2 = CounterDataFrame(
        df[df['Entity Name'] == '034050780000000B'].copy(),
        datetime='Timestamp', 
        counter='eventsCountA', 
        name='Entity Name', 
        timestamp=True
    )
    ## Sensors for the vehicules which leave the parking lot
    cdf_out = CounterDataFrame(
        df[df['Entity Name'] == '034050780000000A'].copy(), 
        datetime='Timestamp', 
        counter='eventsCountB', 
        name='Entity Name', 
        timestamp=True
    )

    # Compute the hourly counts IN/OUT
    ## IN
    ct_hr_in_1 = cdf_in_1.get_hour_counts(clean=True)
    ct_hr_in_2 = cdf_in_2.get_hour_counts(clean=True)
    ## Aggregate data from multiple sensors
    ct_hr_in = ct_hr_in_1.join(ct_hr_in_2, how='inner', lsuffix='_1', rsuffix='_2')
    ct_hr_in['in'] = ct_hr_in.sum(axis=1)
    ## OUT
    ct_hr_out = cdf_out.get_hour_counts()
    ct_hr_out['out'] = ct_hr_out.delta.copy()

    # Join IN and OUT counts into one dataframe
    ct_in_out = ct_hr_in.join(ct_hr_out, how='inner')[['in', 'out']]
    ct_in_out['delta'] = ct_in_out['in'] - ct_in_out['out']
    ct_in_out['nb_veh'] = ct_in_out['delta'].cumsum()
    
    print(ct_in_out.describe().T)

    
if __name__ == '__main__':
    main()
