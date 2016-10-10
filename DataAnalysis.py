con = psycopg2.connect(database = "postgres")
cur = con.cursor()

"""Question1"""
"""get date"""
cur.execute('''SELECT MONTHDAT.TDAYDATE
               FROM (SELECT HOUSEID, PERSONID, SUM(TRPMILES) as DAYMILES, TDAYDATE
                     FROM DAYTRIP
                     WHERE TRPMILES >= 0
                     GROUP BY HOUSEID, PERSONID, TDAYDATE) MONTHDAT
               GROUP BY MONTHDAT.TDAYDATE;''');

get_date = cur.fetchall()
Date_weight = []
for t in get_date:
    month = str(t[0])[-2:]
    if month in ['04','06','09','11']:
        weight = 30
    else:
        if month == '02':
            weight = 28
        else:
            weight = 31
    Date_weight.append((t[0], weight))

Date_weight = (str(Date_weight))[1:-1]    

"""create a separate table with dates and weights"""
cur.execute('DROP TABLE IF EXISTS WEIGHTS')
cur.execute('''CREATE TABLE WEIGHTS(
               TDAYDATE int,
               DAYS int,
               PRIMARY KEY (TDAYDATE));''');

cur.execute('INSERT INTO WEIGHTS (TDAYDATE,DAYS) VALUES'+ Date_weight + ';');
con.commit()

'''join tables MONTHDAT and WEIGHTS'''
'''get total number of weighted individuals'''
cur.execute('''SELECT SUM(DAYS)
               FROM (SELECT HOUSEID, PERSONID, SUM(TRPMILES) as DAYMILES, TDAYDATE
                     FROM DAYTRIP
                     WHERE TRPMILES >= 0
                     GROUP BY HOUSEID, PERSONID, TDAYDATE) as MONTHDAT NATURAL JOIN WEIGHTS
                     ;''');


tot_wtind = cur.fetchall()
tot_wtind = (tot_wtind[0])[0]

result1 = []
for cutoff in range(5,105,5): 
    cur.execute('''SELECT SUM(DAYS)
                   FROM (SELECT HOUSEID, PERSONID, SUM(TRPMILES) as DAYMILES, TDAYDATE
                         FROM DAYTRIP
                         WHERE TRPMILES >= 0
                         GROUP BY HOUSEID, PERSONID, TDAYDATE) as MONTHDAT NATURAL JOIN WEIGHTS
                   WHERE DAYMILES<''' + str(cutoff) +';');
    tot_wt = cur.fetchall()
    result1.append((tot_wt[0])[0]/tot_wtind)
    
result1    


'''Question2'''
result2 = []
for cutoff in range(5,105,5): 
    cur.execute('''SELECT SUM(EPATMPG*DAYS*TRPMILES)/SUM(DAYS*TRPMILES)
                   FROM ((SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                         FROM DAYTRIP
                         WHERE TRPMILES >= 0 AND VEHID >=1 AND DRVR_FLG = '01') as MONTHDAT NATURAL JOIN
                         (SELECT HOUSEID, VEHID, EPATMPG
                          FROM VEHICLE
                          WHERE VEHID >=1) as VEHICLEDAT NATURAL JOIN WEIGHTS)
                   WHERE TRPMILES<''' + str(cutoff) +';');
    tot_wt = cur.fetchall()    
    result2.append((tot_wt[0])[0])
        
result2

'''Question3'''
cur.execute('''SELECT CO2HHVC/CO2_TOT, YYYYMM
               FROM ((SELECT SUM(GALLON)*DAYS*8.887*0.001*117538000/COUNT(DISTINCT(HOUSEID)) as CO2HHVC, TDAYDATE as YYYYMM
                     FROM  (SELECT TRPMILES/EPATMPG as GALLON, HOUSEID, TDAYDATE, DAYS
                            FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                  FROM DAYTRIP
                                  WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                  FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                            WHERE TDAYDATE>=200803 AND TDAYDATE<=200904) as TRIPVC
                     GROUP BY TDAYDATE, DAYS) as PART1 NATURAL JOIN
                     (SELECT VALUE*1000000 as CO2_TOT, YYYYMM
                     FROM ENERGY
                     WHERE YYYYMM>=200803 AND YYYYMM<=200904 AND MSN = 'TEACEUS') as Part2) as Part3;''');

result3 = cur.fetchall()


'''Question4'''
result4_20 = []
cur.execute('''SELECT TOTKWH*MCKWH, TDAYDATE
               FROM ( (SELECT SUM(KWH)*DAYS*117538000/COUNT(DISTINCT(HOUSEID)) TOTKWH, TDAYDATE
                      FROM  (SELECT TRPMILES/(EPATMPG*0.090634441) as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                   (SELECT HOUSEID, VEHID, EPATMPG
                                    FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                             WHERE TRPMILES<=20
                             UNION
                             SELECT 20/EPATMPG*0.090634441 as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM ((SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                   FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS) as E
                             WHERE TRPMILES>20) as tot20
                      GROUP BY TDAYDATE, DAYS) as D NATURAL JOIN
                      (SELECT Value1/Value2 as MCKWH, YYYYMM as TDAYDATE
                      FROM ((SELECT Value as Value1, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'TXEIEUS' AND YYYYMM>=200803 AND YYYYMM<=200904) as A
                            NATURAL JOIN
                            (SELECT Value as Value2, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'ELETPUS') as B) as ELECDAT    )as C) as TOTTBL;''')

result4_el_20 = cur.fetchall()

"""next step is to calculate co2 which could be compared w/"""

cur.execute('''SELECT CO2HHVC, YYYYMM
               FROM (SELECT SUM(GALLON)*DAYS*8.887*0.001*117538000/COUNT(DISTINCT(HOUSEID)) as CO2HHVC, TDAYDATE as YYYYMM
                     FROM  (SELECT TRPMILES/EPATMPG as GALLON, HOUSEID, TDAYDATE, DAYS
                            FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                  FROM DAYTRIP
                                  WHERE VEHID>=1 AND DRVR_FLG = '01' AND TRPMILES <=20) as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                  FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                            WHERE TDAYDATE>=200803 AND TDAYDATE<=200904) as TRIPVC
                     GROUP BY TDAYDATE, DAYS) as PART1;''');
result4_ps_20 = cur.fetchall()

reduction_co2 = []
for i in result4_el_20:
    index = result4_el_20.index(i)
    reduction = i[0]-(result4_ps_20[index])[0]
    result4_20.append((reduction, i[1]))

result4_20

result4_40 = []
cur.execute('''SELECT TOTKWH*MCKWH, TDAYDATE
               FROM ( (SELECT SUM(KWH)*DAYS*117538000/COUNT(DISTINCT(HOUSEID)) TOTKWH, TDAYDATE
                      FROM  (SELECT TRPMILES/(EPATMPG*0.090634441) as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                   (SELECT HOUSEID, VEHID, EPATMPG
                                    FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                             WHERE TRPMILES<=40
                             UNION
                             SELECT 40/EPATMPG*0.090634441 as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM ((SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                   FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS) as E
                             WHERE TRPMILES>40) as tot20
                      GROUP BY TDAYDATE, DAYS) as D NATURAL JOIN
                      (SELECT Value1/Value2 as MCKWH, YYYYMM as TDAYDATE
                      FROM ((SELECT Value as Value1, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'TXEIEUS' AND YYYYMM>=200803 AND YYYYMM<=200904) as A
                            NATURAL JOIN
                            (SELECT Value as Value2, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'ELETPUS') as B) as ELECDAT    )as C) as TOTTBL;''')

result4_el_40 = cur.fetchall()

"""next step is to calculate co2 which could be compared w/"""

cur.execute('''SELECT CO2HHVC, YYYYMM
               FROM (SELECT SUM(GALLON)*DAYS*8.887*0.001*117538000/COUNT(DISTINCT(HOUSEID)) as CO2HHVC, TDAYDATE as YYYYMM
                     FROM  (SELECT TRPMILES/EPATMPG as GALLON, HOUSEID, TDAYDATE, DAYS
                            FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                  FROM DAYTRIP
                                  WHERE VEHID>=1 AND DRVR_FLG = '01' AND TRPMILES <=40) as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                  FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                            WHERE TDAYDATE>=200803 AND TDAYDATE<=200904) as TRIPVC
                     GROUP BY TDAYDATE, DAYS) as PART1;''');
result4_ps_40 = cur.fetchall()


for i in result4_el_40:
    index = result4_el_40.index(i)
    reduction = i[0]-(result4_ps_40[index])[0]
    result4_40.append((reduction, i[1]))

result4_40

result4_60 = []
cur.execute('''SELECT TOTKWH*MCKWH, TDAYDATE
               FROM ( (SELECT SUM(KWH)*DAYS*117538000/COUNT(DISTINCT(HOUSEID)) TOTKWH, TDAYDATE
                      FROM  (SELECT TRPMILES/(EPATMPG*0.090634441) as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                   (SELECT HOUSEID, VEHID, EPATMPG
                                    FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                             WHERE TRPMILES<=60
                             UNION
                             SELECT 60/EPATMPG*0.090634441 as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM ((SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                   FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS) as E
                             WHERE TRPMILES>60) as tot20
                      GROUP BY TDAYDATE, DAYS) as D NATURAL JOIN
                      (SELECT Value1/Value2 as MCKWH, YYYYMM as TDAYDATE
                      FROM ((SELECT Value as Value1, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'TXEIEUS' AND YYYYMM>=200803 AND YYYYMM<=200904) as A
                            NATURAL JOIN
                            (SELECT Value as Value2, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'ELETPUS') as B) as ELECDAT    )as C) as TOTTBL;''')

result4_el_60 = cur.fetchall()

"""next step is to calculate co2 which could be compared w/"""

cur.execute('''SELECT CO2HHVC, YYYYMM
               FROM (SELECT SUM(GALLON)*DAYS*8.887*0.001*117538000/COUNT(DISTINCT(HOUSEID)) as CO2HHVC, TDAYDATE as YYYYMM
                     FROM  (SELECT TRPMILES/EPATMPG as GALLON, HOUSEID, TDAYDATE, DAYS
                            FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                  FROM DAYTRIP
                                  WHERE VEHID>=1 AND DRVR_FLG = '01' AND TRPMILES <=60) as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                  FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                            WHERE TDAYDATE>=200803 AND TDAYDATE<=200904) as TRIPVC
                     GROUP BY TDAYDATE, DAYS) as PART1;''');
result4_ps_60 = cur.fetchall()

reduction_co2 = []
for i in result4_el_60:
    index = result4_el_60.index(i)
    reduction = i[0]-(result4_ps_60[index])[0]
    result4_60.append((reduction, i[1]))

result4_60
'''Questions5a'''
result = []
cutoff = [84,107,208,270]
for i in cutoff:
    result_el = 0
    result_ps = 0
    cur.execute('''SELECT TOTKWH*MCKWH, TDAYDATE
               FROM ((SELECT SUM(KWH)*DAYS*117538000/COUNT(DISTINCT(HOUSEID)) TOTKWH, TDAYDATE
                      FROM  (SELECT TRPMILES/(EPATMPG*0.090634441) as KWH, HOUSEID, TDAYDATE, DAYS
                             FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                   FROM DAYTRIP
                                   WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                                   (SELECT HOUSEID, VEHID, EPATMPG
                                    FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                             WHERE TRPMILES<='''+ str(i) + ') as tot'
                   ''' GROUP BY TDAYDATE, DAYS) as D NATURAL JOIN
                      (SELECT Value1/Value2 as MCKWH, YYYYMM as TDAYDATE
                      FROM ((SELECT Value as Value1, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'TXEIEUS' AND YYYYMM>=200803 AND YYYYMM<=200904) as A
                            NATURAL JOIN
                            (SELECT Value as Value2, YYYYMM
                            FROM ENERGY
                            WHERE MSN = 'ELETPUS') as B) as ELECDAT)as C) as TOTTBL;''')
     result_el = cur.fetchall()
     
     cur.execute('''SELECT CO2HHVC, YYYYMM
               FROM (SELECT SUM(GALLON)*DAYS*8.887*0.001*117538000/COUNT(DISTINCT(HOUSEID)) as CO2HHVC, TDAYDATE as YYYYMM
                     FROM  (SELECT TRPMILES/EPATMPG as GALLON, HOUSEID, TDAYDATE, DAYS
                            FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                  FROM DAYTRIP
                                  WHERE VEHID>=1 AND DRVR_FLG = '01' AND TRPMILES <''' + str(i) +''') as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                  FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                            WHERE TDAYDATE>=200803 AND TDAYDATE<=200904) as TRIPVC
                     GROUP BY TDAYDATE, DAYS) as PART1;''');
     result_ps = cur.fetchall()

     for j in result_el:
         index = result_el.index(j)    
         co2_reduction_pct = (j[0]-(result_ps[index])[0])/(result_ps[index])[0]
         result.append((co2_reduction_pct, (result_el[index])[1]))

result

'''Questions5b'''
result_el = []
result2 = []
cutoff = [84,107,208,270]
for i in cutoff:
    cur.execute('''SELECT SUM(KWH)*DAYS*117538000/COUNT(DISTINCT(HOUSEID)), TDAYDATE
                   FROM  (SELECT TRPMILES/(EPATMPG*0.090634441) as KWH, HOUSEID, TDAYDATE, DAYS
                          FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                FROM DAYTRIP
                                WHERE VEHID>=1 AND DRVR_FLG = '01') as TRIPDAT NATURAL JOIN
                               (SELECT HOUSEID, VEHID, EPATMPG
                                FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                          WHERE TRPMILES<='''+ str(i) + ''') as tot
                   GROUP BY TDAYDATE, DAYS;''')
    result_el1 = cur.fetchall()

    cur.execute('''SELECT Value1/Value2 as MCKWH, YYYYMM
                   FROM ((SELECT Value as Value1, YYYYMM
                          FROM ENERGY
                          WHERE MSN = 'TXEIEUS' AND YYYYMM>=201401 AND YYYYMM<=201412) as co21
                          NATURAL JOIN
                          (SELECT SUM(Value) as Value2, YYYYMM
                           FROM (SELECT Value, YYYYMM
                                 FROM ENERGY
                                 WHERE MSN = 'NGETPUS' OR MSN = 'NUETPUS' OR MSN = 'WYETPUS') as ELEC1
                           GROUP BY YYYYMM) as ELEC2 )as CO2KMG;''')
    result_el2 = cur.fetchall()

    for ii in result_el1:
        for j in result_el2:
            if str(ii[1])[-2:] == str(j[1])[-2:]:
               co2 = ii[0]*j[0]
               result_el.append((co2, ii[1]))
               
    cur.execute('''SELECT CO2HHVC, YYYYMM
               FROM (SELECT SUM(GALLON)*DAYS*8.887*0.001*117538000/COUNT(DISTINCT(HOUSEID)) as CO2HHVC, TDAYDATE as YYYYMM
                     FROM  (SELECT TRPMILES/EPATMPG as GALLON, HOUSEID, TDAYDATE, DAYS
                            FROM (SELECT HOUSEID, VEHID, TRPMILES, TDAYDATE
                                  FROM DAYTRIP
                                  WHERE VEHID>=1 AND DRVR_FLG = '01' AND TRPMILES <''' + str(i) +''') as TRIPDAT NATURAL JOIN
                                  (SELECT HOUSEID, VEHID, EPATMPG
                                  FROM VEHICLE) as VEHDAT NATURAL JOIN WEIGHTS
                            WHERE TDAYDATE>=200803 AND TDAYDATE<=200904) as TRIPVC
                     GROUP BY TDAYDATE, DAYS) as PART1;''');
     result_ps = cur.fetchall()
     result_el = result_el[0:14]
     for j in result_el:
         index = result_el.index(j)    
         co2_reduction_pct = (j[0]-(result_ps[index])[0])/(result_ps[index])[0]
         result2.append((co2_reduction_pct, (result_el[index])[1]))

result2

con.commit()
con.close()
