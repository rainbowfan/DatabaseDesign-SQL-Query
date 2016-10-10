import psycopg2
import os
import csv

user = os.environ['USER']

con = psycopg2.connect(database = "postgres")

cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS HOUSEHOLD')
cur.execute('''CREATE TABLE HOUSEHOLD
               (HOUSEID varchar(8),
                VARSTRAT int,
                WTHHFIN float,
                DRVRCNT int,
                CDIVMSAR varchar(2),
                CENSUS_D varchar(2),
                CENSUS_R varchar(2),
                HH_HISP varchar(2),
                HH_RACE varchar(2),
                HHFAMINC varchar(2),
                HHRELATD varchar(2),
                HHRESP varchar(2),
                HHSIZE int,
                HHSTATE varchar(2),
                HHSTFIPS varchar(2),
                HHVEHCNT int,
                HOMEOWN varchar(2),
                HOMETYPE varchar(2),
                MSACAT varchar(2),
                MSASIZE varchar(2),
                NUMADLT int,
                RAIL varchar(2),
                RESP_CNT int,
                SCRESP varchar(2),
                TRAVDAY varchar(2),
                URBAN varchar(2),
                URBANSIZE varchar(2),
                URBRUR varchar(2),
                WRKCOUNT int,
                TDAYDATE int,
                FLAG100 varchar(2),
                LIF_CYC varchar(2),
                CNTTDHH varchar(2),
                HBHUR varchar(2),
                HTRESDN varchar(5),
                HTHTNRNT varchar(2),
                HTPPOPDN varchar(5), 
                HTEEMPDN varchar(4),
                HBRESDN varchar(5),
                HBHTNRNT varchar(2),
                HBPPOPDN varchar(5),
                HH_CBSA varchar(5),
                HHC_MSA varchar(4),
                PRIMARY KEY (HOUSEID)
                );''');

cur.execute('DROP TABLE IF EXISTS DAYTRIP')
cur.execute('''CREATE TABLE DAYTRIP
               (HOUSEID varchar(8),
                PERSONID varchar(2),
                FRSTHM varchar(2),
                OUTOFTWN varchar(2),
                ONTD_P1 varchar(2),
                ONTD_P2 varchar(2),
                ONTD_P3 varchar(2),
                ONTD_P4 varchar(2),
                ONTD_P5 varchar(2),
                ONTD_P6 varchar(2),
                ONTD_P7 varchar(2),
                ONTD_P8 varchar(2),
                ONTD_P9 varchar(2),
                ONTD_P10 varchar(2),
                ONTD_P11 varchar(2),
                ONTD_P12 varchar(2),
                ONTD_P13 varchar(2),
                ONTD_P14 varchar(2),
                ONTD_P15 varchar(2),
                TDCASEID varchar(12),
                HH_HISP varchar(2),
                HH_RACE varchar(2),
                DRIVER varchar(2),
                R_SEX varchar(2),
                WORKER varchar(2),
                DRVRCNT int,
                HHFAMINC varchar(2),
                HHSIZE int,
                HHVEHCNT int,
                NUMADLT int,
                FLAG100 varchar(2),
                LIF_CYC varchar(2),
                TRIPPURP varchar(8),
                AWAYHOME varchar(2),
                CDIVMSAR varchar(2),
                CENSUS_D varchar(2),
                CENSUS_R varchar(2),
                DROP_PRK varchar(8),
                DRVR_FLG varchar(2),
                EDUC varchar(2),
                ENDTIME varchar(4),
                HH_ONTD int,
                HHMEMDRV varchar(2),
                HHRESP varchar(2),
                HHSTATE varchar(2),
                HHSTFIPS varchar(2),
                INTSTATE varchar(2),
                MSACAT varchar(2),
                MSASIZE varchar(2),
                NONHHCNT int,
                NUMONTRP int,
                PAYTOLL varchar(2),
                PRMACT varchar(2),
                PROXY varchar(2),
                PSGR_FLG varchar(2),
                R_AGE int,
                RAIL varchar(2),
                STRTTIME varchar(4),
                TRACC1 varchar(2),
                TRACC2 varchar(2),
                TRACC3 varchar(2),
                TRACC4 varchar(2),
                TRACC5 varchar(2),
                TRACCTM int,
                TRAVDAY varchar(2),
                TREGR1 varchar(2),
                TREGR2 varchar(2),
                TREGR3 varchar(2),
                TREGR4 varchar(2),
                TREGR5 varchar(2),
                TREGRTM int,
                TRPACCMP int,
                TRPHHACC int,
                TRPHHVEH varchar(2),
                TRPTRANS varchar(2),
                TRVL_MIN int,
                TRVLCMIN int,
                TRWAITTM int,
                URBAN varchar(2),
                URBANSIZE varchar(2),
                URBRUR varchar(2),
                USEINTST varchar(2),
                USEPUBTR varchar(2),
                VEHID int,
                WHODROVE varchar(2),
                WHYFROM varchar(2),
                WHYTO varchar(2),
                WHYTRP1S varchar(2),
                WRKCOUNT int,
                DWELTIME int,
                WHYTRP90 varchar(2),
                TDTRPNUM varchar(12),
                TDWKND varchar(2),
                TDAYDATE int,
                TRPMILES float, 
                WTTRDFIN float,
                VMT_MILE float,
                PUBTRANS varchar(2),
                HOMEOWN varchar(2),
                HOMETYPE varchar(2),
                HBHUR varchar(2),
                HTRESDN varchar(5),
                HTHTNRNT varchar(2),
                HTPPOPDN varchar(5),
                HTEEMPDN varchar(4),
                HBRESDN varchar(5),
                HBHTNRNT varchar(2),
                HBPPOPDN varchar(5),
                GASPRICE float,
                VEHTYPE varchar(3),
                HH_CBSA varchar(5),
                HHC_MSA varchar(4),
                PRIMARY KEY (TDCASEID)
                );''');

cur.execute('DROP TABLE IF EXISTS PERSON')
cur.execute('''CREATE TABLE PERSON
               (HOUSEID varchar(8),
                PERSONID varchar(2),
                VARSTRAT int,
                WTPERFIN float,
                SFWGT float,
                HH_HISP varchar(2),
                HH_RACE varchar(2),
                DRVRCNT int,
                HHFAMINC varchar(2),
                HHSIZE int,
                HHVEHCNT int,
                NUMADLT int,
                WRKCOUNT int,
                FLAG100 varchar(2),
                LIF_CYC varchar(2),
                CNTTDTR int,
                BORNINUS varchar(2),
                CARRODE int,
                CDIVMSAR varchar(2),
                CENSUS_D varchar(2),
                CENSUS_R varchar(2),
                CONDNIGH varchar(2),
                CONDPUB varchar(2),
                CONDRIDE varchar(2),
                CONDRIVE varchar(2),
                CONDSPEC varchar(2),
                CONDTAX varchar(2),
                CONDTRAV varchar(2),
                DELIVER int,
                DIARY varchar(2),
                DISTTOSC varchar(2),
                DRIVER varchar(2),
                DTACDT varchar(2),
                DTCONJ varchar(2),
                DTCOST varchar(2),
                DTRAGE varchar(2),
                DTRAN varchar(2),
                DTWALK varchar(2),
                EDUC varchar(2),
                EVERDROV varchar(2),
                FLEXTIME varchar(2),
                FMSCSIZE int,
                FRSTHM varchar(2),
                FXDWKPL varchar(2),
                GCDWORK float,
                GRADE varchar(2),
                GT1JBLWK varchar(2),
                HHRESP varchar(2),
                HHSTATE varchar(2),
                HHSTFIPS varchar(2),
                ISSUE varchar(2),
                OCCAT varchar(2),
                LSTTRDAY int,
                MCUSED float,
                MEDCOND varchar(2),
                MEDCOND6 varchar(2),
                MOROFTEN varchar(2),
                MSACAT varchar(2),
                MSASIZE varchar(2),
                NBIKETRP int,
                NWALKTRP int,
                OUTCNTRY varchar(2),
                OUTOFTWN varchar(2),
                PAYPROF varchar(2),
                PRMACT varchar(2),
                PROXY varchar(2),
                PTUSED int,
                PURCHASE int,
                R_AGE int,
                R_RELAT varchar(2),
                R_SEX varchar(2),
                RAIL varchar(2),
                SAMEPLC varchar(2),
                SCHCARE varchar(2),
                SCHCRIM varchar(2),
                SCHDIST varchar(2),
                SCHSPD varchar(2),
                SCHTRAF varchar(2),
                SCHTRN1 varchar(2),
                SCHTRN2 varchar(2),
                SCHTYP varchar(2),
                SCHWTHR varchar(2),
                SELF_EMP varchar(2),
                TIMETOSC float,
                TIMETOWK float,
                TOSCSIZE int,
                TRAVDAY varchar(2),
                URBAN varchar(2),
                URBANSIZE varchar(2),
                URBRUR varchar(2),
                USEINTST varchar(2),
                USEPUBTR varchar(2),
                WEBUSE varchar(2),
                WKFMHMXX int,
                WKFTPT varchar(2),
                WKRMHM varchar(2),
                WKSTFIPS varchar(2),
                WORKER varchar(2),
                WRKTIME varchar(8),
                WRKTRANS varchar(2),
                YEARMILE float,
                YRMLCAP varchar(2),
                YRTOUS int,
                DISTTOWK float,
                TDAYDATE int,
                HOMEOWN varchar(2),
                HOMETYPE varchar(2),
                HBHUR varchar(2),
                HTRESDN varchar(5),
                HTHTNRNT varchar(2),
                HTPPOPDN varchar(5),
                HTEEMPDN varchar(4),
                HBRESDN varchar(5),
                HBHTNRNT varchar(2),
                HBPPOPDN varchar(5),
                HH_CBSA varchar(5),
                HHC_MSA varchar(4),
                PRIMARY KEY (HOUSEID, PERSONID)
                );''');

cur.execute('DROP TABLE IF EXISTS VEHICLE')
cur.execute('''CREATE TABLE VEHICLE
               (HOUSEID varchar(8),
                WTHHFIN float,
                VEHID int,
                DRVRCNT int,
                HHFAMINC varchar(2),
                HHSIZE int,
                HHVEHCNT int,
                NUMADLT int,
                FLAG100 varchar(2),
                CDIVMSAR varchar(2),
                CENSUS_D varchar(2),
                CENSUS_R varchar(2),
                HHSTATE varchar(2),
                HHSTFIPS varchar(2),
                HYBRID varchar(2),
                MAKECODE varchar(2),
                MODLCODE varchar(3),
                MSACAT varchar(2),
                MSASIZE varchar(2),
                OD_READ float,
                RAIL varchar(2),
                TRAVDAY varchar(2),
                URBAN varchar(2),
                URBANSIZE varchar(2),
                URBRUR varchar(2),
                VEHCOMM varchar(2),
                VEHOWNMO float,
                VEHYEAR int,
                WHOMAIN varchar(2),
                WRKCOUNT int,
                TDAYDATE int,
                VEHAGE int,
                PERSONID varchar(2),
                HH_HISP varchar(2),
                HH_RACE varchar(2),
                HOMEOWN varchar(2),
                HOMETYPE varchar(2),
                LIF_CYC varchar(2),
                ANNMILES float,
                HBHUR varchar(2),
                HTRESDN varchar(5),
                HTHTNRNT varchar(2),
                HTPPOPDN varchar(5),
                HTEEMPDN varchar(4),
                HBRESDN varchar(5),
                HBHTNRNT varchar(2),
                HBPPOPDN varchar(5),
                BEST_FLG varchar(2),
                BESTMILE float,
                BEST_EDT varchar(2),
                BEST_OUT varchar(2),
                FUELTYPE float,
                GSYRGAL float,
                GSCOST float,
                GSTOTCST float,
                EPATMPG float,
                EPATMPGF varchar(2),
                EIADMPG float,
                VEHTYPE varchar(3),
                HH_CBSA varchar(5),
                HHC_MSA varchar(4),
                PRIMARY KEY (HOUSEID, VEHID)
                );''');

HHattr = '(HOUSEID,VARSTRAT,WTHHFIN,DRVRCNT,CDIVMSAR,CENSUS_D,CENSUS_R,HH_HISP,HH_RACE,HHFAMINC,HHRELATD,HHRESP,HHSIZE,HHSTATE,HHSTFIPS,HHVEHCNT,HOMEOWN,HOMETYPE,MSACAT,MSASIZE,NUMADLT,RAIL,RESP_CNT,SCRESP,TRAVDAY,URBAN,URBANSIZE,URBRUR,WRKCOUNT,TDAYDATE,FLAG100,LIF_CYC,CNTTDHH,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,HH_CBSA,HHC_MSA)'
DTattr = '(HOUSEID,PERSONID,FRSTHM,OUTOFTWN,ONTD_P1,ONTD_P2,ONTD_P3,ONTD_P4,ONTD_P5,ONTD_P6,ONTD_P7,ONTD_P8,ONTD_P9,ONTD_P10,ONTD_P11,ONTD_P12,ONTD_P13,ONTD_P14,ONTD_P15,TDCASEID,HH_HISP,HH_RACE,DRIVER,R_SEX,WORKER,DRVRCNT,HHFAMINC,HHSIZE,HHVEHCNT,NUMADLT,FLAG100,LIF_CYC,TRIPPURP,AWAYHOME,CDIVMSAR,CENSUS_D,CENSUS_R,DROP_PRK,DRVR_FLG,EDUC,ENDTIME,HH_ONTD,HHMEMDRV,HHRESP,HHSTATE,HHSTFIPS,INTSTATE,MSACAT,MSASIZE,NONHHCNT,NUMONTRP,PAYTOLL,PRMACT,PROXY,PSGR_FLG,R_AGE,RAIL,STRTTIME,TRACC1,TRACC2,TRACC3,TRACC4,TRACC5,TRACCTM,TRAVDAY,TREGR1,TREGR2,TREGR3,TREGR4,TREGR5,TREGRTM,TRPACCMP,TRPHHACC,TRPHHVEH,TRPTRANS,TRVL_MIN,TRVLCMIN,TRWAITTM,URBAN,URBANSIZE,URBRUR,USEINTST,USEPUBTR,VEHID,WHODROVE,WHYFROM,WHYTO,WHYTRP1S,WRKCOUNT,DWELTIME,WHYTRP90,TDTRPNUM,TDWKND,TDAYDATE,TRPMILES,WTTRDFIN,VMT_MILE,PUBTRANS,HOMEOWN,HOMETYPE,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,GASPRICE,VEHTYPE,HH_CBSA,HHC_MSA)'
PSattr = '(HOUSEID,PERSONID,VARSTRAT,WTPERFIN,SFWGT,HH_HISP,HH_RACE,DRVRCNT,HHFAMINC,HHSIZE,HHVEHCNT,NUMADLT,WRKCOUNT,FLAG100,LIF_CYC,CNTTDTR,BORNINUS,CARRODE,CDIVMSAR,CENSUS_D,CENSUS_R,CONDNIGH,CONDPUB,CONDRIDE,CONDRIVE,CONDSPEC,CONDTAX,CONDTRAV,DELIVER,DIARY,DISTTOSC,DRIVER,DTACDT,DTCONJ,DTCOST,DTRAGE,DTRAN,DTWALK,EDUC,EVERDROV,FLEXTIME,FMSCSIZE,FRSTHM,FXDWKPL,GCDWORK,GRADE,GT1JBLWK,HHRESP,HHSTATE,HHSTFIPS,ISSUE,OCCAT,LSTTRDAY,MCUSED,MEDCOND,MEDCOND6,MOROFTEN,MSACAT,MSASIZE,NBIKETRP,NWALKTRP,OUTCNTRY,OUTOFTWN,PAYPROF,PRMACT,PROXY,PTUSED,PURCHASE,R_AGE,R_RELAT,R_SEX,RAIL,SAMEPLC,SCHCARE,SCHCRIM,SCHDIST,SCHSPD,SCHTRAF,SCHTRN1,SCHTRN2,SCHTYP,SCHWTHR,SELF_EMP,TIMETOSC,TIMETOWK,TOSCSIZE,TRAVDAY,URBAN,URBANSIZE,URBRUR,USEINTST,USEPUBTR,WEBUSE,WKFMHMXX,WKFTPT,WKRMHM,WKSTFIPS,WORKER,WRKTIME,WRKTRANS,YEARMILE,YRMLCAP,YRTOUS,DISTTOWK,TDAYDATE,HOMEOWN,HOMETYPE,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,HH_CBSA,HHC_MSA)'
VCattr = '(HOUSEID,WTHHFIN,VEHID,DRVRCNT,HHFAMINC,HHSIZE,HHVEHCNT,NUMADLT,FLAG100,CDIVMSAR,CENSUS_D,CENSUS_R,HHSTATE,HHSTFIPS,HYBRID,MAKECODE,MODLCODE,MSACAT,MSASIZE,OD_READ,RAIL,TRAVDAY,URBAN,URBANSIZE,URBRUR,VEHCOMM,VEHOWNMO,VEHYEAR,WHOMAIN,WRKCOUNT,TDAYDATE,VEHAGE,PERSONID,HH_HISP,HH_RACE,HOMEOWN,HOMETYPE,LIF_CYC,ANNMILES,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,BEST_FLG,BESTMILE,BEST_EDT,BEST_OUT,FUELTYPE,GSYRGAL,GSCOST,GSTOTCST,EPATMPG,EPATMPGF,EIADMPG,VEHTYPE,HH_CBSA,HHC_MSA)'

attr = [HHattr, DTattr, PSattr, VCattr]
filename_NHTS = ['HHV2PUB.CSV', 'DAYV2PUB.CSV', 'PERV2PUB.CSV', 'VEHV2PUB.CSV']
tblname_NHTS = ['HOUSEHOLD', 'DAYTRIP', 'PERSON', 'VEHICLE']

for i in range(4):
    with open('/Users/hongfan/Downloads/NHTS_2011_Ascii/Ascii/'+filename_NHTS[i], 'r') as file:
         file.readline()
         reader = csv.reader(file)
         result = list(map(tuple, reader))

    thousand = int(len(result)/1000) + 1

    for j in range(thousand):
            if j != thousand - 1:
               end = (j+1)*1000
            else:
               end = len(result) - 1

            part = result[(j*1000) : end]
            part_new = (str(part))[1:-1]              
            cur.execute('INSERT INTO ' + tblname_NHTS[i] + attr[i] + 'VALUES' + part_new + ';');

            
cur.execute('DROP TABLE IF EXISTS MULTIUNIT')
cur.execute('''CREATE TABLE MULTIUNIT
               (MSN varchar(8),
                Column_Order int,
                Description varchar,
                Unit varchar,
                Primary Key (MSN, Column_Order, Description, Unit));'''
                );

cur.execute('DROP TABLE IF EXISTS ENERGY')               
cur.execute('''CREATE TABLE ENERGY
               (MSN varchar(8),
                YYYYMM int,
                Value float,
                Primary Key (MSN, YYYYMM));''');

energy_name = '(MSN, YYYYMM, Value)' 
unit_name = '(MSN, Column_Order, Description, Unit)'


file_name = ['EIA_CO2_Transportation_2015.csv', 'EIA_CO2_Electricity_2015.csv',
             'EIA_MkWh_2015.csv']

unit = []
for i in range(3):
    energy = []  
    with open('/Users/hongfan/Downloads/'+file_name[i], 'r') as file:
         file.readline()
         reader = csv.reader(file)
         result = list(map(tuple, reader))
    for t in result:
        energy.append(t[0:3])
        unit.append((t[0], t[3], t[4], t[5]))       
    thousand = int(len(result)/1000) + 1
    for j in range(thousand):
        if j != thousand - 1:
            end = (j+1)*1000
        else:
            end = len(result)-1
        energy_part = energy[(j*1000) : end]
        energy_part_new = (str(energy_part))[1:-1]
        energy_part_new = energy_part_new.replace("'Not Available'", "NULL", 3*1000)
        cur.execute('INSERT INTO ENERGY'+ energy_name + 'VALUES' + energy_part_new + ';');

unit = list(set(unit)) 
unit_part_new = (str(unit))[1:-1]
unit_part_new = unit_part_new.replace("'Not Available'", "NULL")
cur.execute('INSERT INTO MULTIUNIT'+ unit_name + 'VALUES' + unit_part_new + ';');
       
con.commit()

