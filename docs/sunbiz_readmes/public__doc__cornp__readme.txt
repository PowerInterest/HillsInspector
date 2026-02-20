	2000 4th  Quarter  CORPORATIONS

	This data file consists of 10 subsets which are on 
	five (5) CD's that contain two data files each.  
	The data files are labeled CORDATA0.DAT thru CORDATA9.DAT.  
	These are data files containing all the active and inactive corporations 
	from the Florida Secretary of State.  Included in this README.TXT 
	is the file description of the data file, along with a brief 
	description of each data element.


FD  ANNUAL_MICRO_DATA_FILE
    RECORD CONTAINS 1170 CHARACTERS
    DATA RECORD IS ANNUAL_MICRO_DATA_REC.

01  ANNUAL_MICRO_DATA_REC.
    03  ANNUAL_COR_NUMBER                     PIC X(12).
    03  ANNUAL_COR_NAME                       PIC X(48).
    03  ANNUAL_COR_STATUS                     PIC X(01).
    03  ANNUAL_COR_FILING_TYPE                PIC X(15).
    03  ANNUAL_COR_2ND_MAIL_ADD_1             PIC X(42).
    03  ANNUAL_COR_2ND_MAIL_ADD_2             PIC X(42).
    03  ANNUAL_COR_2ND_MAIL_CITY              PIC X(28).
    03  ANNUAL_COR_2ND_MAIL_STATE             PIC X(02).
    03  ANNUAL_COR_2ND_MAIL_ZIP               PIC X(10).
    03  ANNUAL_COR_2ND_MAIL_COUNTRY           PIC X(02).
    03  ANNUAL_COR_FILE_DATE                  PIC X(08).
    03  ANNUAL_COR_FEI_NUMBER                 PIC X(14).
    03  ANNUAL_MORE_THAN_SIX_OFF_FLAG         PIC X(01).
    03  ANNUAL_LAST_TRX_DATE                  PIC X(08).
    03  ANNUAL_STATE_COUNTRY                  PIC X(02).
    03  ANNUAL_REPORT_YEAR_1                  PIC X(04).
    03  ANNUAL_HOUSE_FLAG_1                   PIC X(01).
    03  ANNUAL_REPORT_DATE_1                  PIC X(08).
    03  ANNUAL_REPORT_YEAR_2                  PIC X(04).
    03  ANNUAL_HOUSE_FLAG_2                   PIC X(01).
    03  ANNUAL_REPORT_DATE_2                  PIC X(08).
    03  ANNUAL_REPORT_YEAR_3                  PIC X(04).
    03  ANNUAL_HOUSE_FLAG_3                   PIC X(01).
    03  ANNUAL_REPORT_DATE_3                  PIC X(08).
    03  ANNUAL_RA_NAME                        PIC X(42).
    03  ANNUAL_RA_NAME_TYPE                   PIC X(01).
    03  ANNUAL_RA_ADD_1                       PIC X(42).
    03  ANNUAL_RA_CITY                        PIC X(28).
    03  ANNUAL_RA_STATE                       PIC X(02).
    03  ANNUAL_RA_ZIP5                        PIC X(05).
    03  ANNUAL_RA_ZIP4                        PIC X(04).
    03  ANNUAL_PRINCIPALS                     OCCURS 6 TIMES.
        05  ANNUAL_PRINC_TITLE                PIC X(04).
        05  ANNUAL_PRINC_NAME_TYPE            PIC X(01).
        05  ANNUAL_PRINC_NAME                 PIC X(42).
        05  ANNUAL_PRINC_ADD_1                PIC X(42).
        05  ANNUAL_PRINC_CITY                 PIC X(28).
        05  ANNUAL_PRINC_STATE                PIC X(02).
        05  ANNUAL_PRINC_ZIP5                 PIC X(05).
        05  ANNUAL_PRINC_ZIP4                 PIC X(04).
    03  FILLER                                PIC X(04).


          

    The following is an explanation of what each field contains.

    ANNUAL_COR_NUMBER          : The corporate document number
    ANNUAL_COR_NAME            : The 1st 48 characters of the corporate name
    ANNUAL_COR_STATUS          : Corporate status Values are
                                  "A" (active) & "I" (inactive)
    ANNUAL_COR_FILING_TYPE     : Type of filing, Values are:
                                 "DOMP"  - Domestic for Profit
                                 "DOMNP" - Domestic Non Profit
                                 "FORP"  - Foreign for Profit
                                 "FORNP" - Foreign Non Profit
                                 "DOMLP" - Domestic Limited Partnership
                                 "FORLP" - Foreign Limited Partnership
                                 "FLAL"  - Florida Limited Liability
                                 "FORL"  - Foreign Limited Liability
                                 "NPREG" - Non Profit, Regulated
                                 "TRUST" - Declaration of Trust
                                 "AGENT" - Declaration of Registered Agent

    ANNUAL_COR_2ND_MAIL_ADD_1  : The next six (6) fields are the corporations
    ANNUAL_COR_2ND_MAIL_ADD_2    mailing address
    ANNUAL_COR_2ND_MAIL_CITY
    ANNUAL_COR_2ND_MAIL_STATE
    ANNUAL_COR_2ND_MAIL_ZIP
    ANNUAL_COR_2ND_MAIL_COUNTRY

    ANNUAL_COR_FILE_DATE          : The date the corporation was registered
    ANNUAL_COR_FEI_NUMBER         : Federal Employee ID number
    ANNUAL_MORE_THAN_SIX_OFF_FLAG : If the corporation has more than 6 officers
    ANNUAL_LAST_TRX_DATE          : The date of the corps last activity
    ANNUAL_STATE_COUNTRY          : The state of country of origination

                     The next nine fields contain Annual Report information
                     that covers the last three years of activity
                             Report Year : year of report filed
                             House Flag  : N/A
                             Report Date : Date on which AR was filed
    ANNUAL_REPORT_YEAR_1   
    ANNUAL_HOUSE_FLAG_1
    ANNUAL_REPORT_DATE_1
    ANNUAL_REPORT_YEAR_2
    ANNUAL_HOUSE_FLAG_2
    ANNUAL_REPORT_DATE_2
    ANNUAL_REPORT_YEAR_3
    ANNUAL_HOUSE_FLAG_3
    ANNUAL_REPORT_DATE_3
    ANNUAL_RA_NAME                  :  Registered Agent's name
    ANNUAL_RA_NAME_TYPE             :  A "P" (person) or "C" (corporation)
    ANNUAL_RA_ADD_1                 :  RA Street Address
    ANNUAL_RA_CITY                  :  RA City
    ANNUAL_RA_STATE                 :  RA state
    ANNUAL_RA_ZIP5                  :  RA zip (1st 5)
    ANNUAL_RA_ZIP4                  :  RA zip (last 4)
    ANNUAL_PRINCIPALS               :  OCCURS 6 TIMES.
    ANNUAL_PRINC_TITLE              : Officer Title
                              P (president)    T (Treasurer)  C (Chairman) 
                              V (Vice Pres)    S (Secretary)  D (Director)
  
    ANNUAL_PRINC_NAME_TYPE          : A "P" (person) or "C" (corporation)
    ANNUAL_PRINC_NAME               : Officer name
    ANNUAL_PRINC_ADD_1              : Officer Street Address
    ANNUAL_PRINC_CITY               : Officer City  
    ANNUAL_PRINC_STATE              : Officer state
    ANNUAL_PRINC_ZIP5               : Officer zip (1st 5)
    ANNUAL_PRINC_ZIP4               : Officer zip (last 4)


     