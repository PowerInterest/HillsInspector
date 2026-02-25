# Consolidation Document

This document is a consolidated reference composed of the following historical files:
- `sunbiz_COREVENTREADME.txt`
- `sunbiz_FLRreadme.txt`
- `sunbiz_Ficreadme.txt`
- `sunbiz_cor_99-README.TXT`
- `sunbiz_cor_Filings_README.TXT`
- `sunbiz_cor_README.TXT`
- `sunbiz_corindex.txt`
- `sunbiz_cornp_readme.txt`
- `sunbiz_entity_file_layout.md`

---



## Source: sunbiz_COREVENTREADME.txt



             CORPORATIONS EVENTS


        This disc contains a  data file labeled COREVT.EXE. This 
        is a self expanding zip file that contains a file called
        COREVT.dat.  By double clicking on the .exe file the unzip program
        will launch and ask you where you would like the file loaded.
        The expanded file size will be approximately 1.9 billion bytes.
        This file contains the events (changes) that have been filed against the 
	original filings from the Florida Secretary of State.


        Included in this README.TXT is the file description of
        the data file, along with a brief description of each data
        element.


FD  COR_EVENT_FILE
    BLOCK CONTAINS 250 RECORDS
    RECORD CONTAINS 662 CHARACTERS
    DATA RECORD IS COR_EVENT_DATA_REC.

01  COR_EVENT_DATA_REC.                         
    03  COR_EVENT_DOC_NUMBER                      PIC X(12).
    03  COR_EVENT_SEQ_NUMBER                      PIC 9(05).
    03  COR_EVENT_CODE                            PIC X(20).
    03  COR_EVENT_DESC                            PIC X(40).
    03  COR_EVENT_EFFT_DATE                       PIC X(08).
    03  COR_EVENT_FILED_DATE                      PIC X(08).
    03  COR_EVENT_NOTE_1                          PIC X(35).
    03  COR_EVENT_NOTE_2                          PIC X(35).
    03  COR_EVENT_NOTE_3                          PIC X(35).
    03  COR_EVENT_CONS_MER_NUMBER                 PIC X(12).
    03  COR_EVENT_COR_NAME                        PIC X(192).
    03  COR_EVENT_NAME_SEQ                        PIC 9(05).
    03  COR_EVENT_X_NAME_SEQ                      PIC 9(05).
    03  COR_EVENT_NAME_CHG                        PIC X(01).
    03  COR_EVENT_X_NAME_CHG                      PIC X(01).
    03  COR_EVENT_ADD_1                           PIC X(42).
    03  COR_EVENT_ADD_2                           PIC X(42).
    03  COR_EVENT_CITY                            PIC X(28).
    03  COR_EVENT_STATE                           PIC X(02).
    03  COR_EVENT_ZIP                             PIC X(10).
    03  COR_EVENT_MAIL_ADD_1                      PIC X(42).
    03  COR_EVENT_MAIL_ADD_2                      PIC X(42).
    03  COR_EVENT_MAIL_CITY                       PIC X(28).
    03  COR_EVENT_MAIL_STATE                      PIC X(02).
    03  COR_EVENT_MAIL_ZIP                        PIC X(10).


The following is a brief description of what each field contains.

   COR_EVENT_DOC_NUMBER              :  Charter number        
   COR_EVENT_SEQ_NUMBER              :  Event sequence      
   COR_EVENT_CODE                    :  Event code
   COR_EVENT_DESC                    :  Descprition of event       
   COR_EVENT_EFFT_DATE               :  Date event will become effective        
   COR_EVENT_FILED_DATE              :  Date event was filed         
   COR_EVENT_NOTE_1                  :  Event note field 1       
   COR_EVENT_NOTE_2                  :  Event note field 2        
   COR_EVENT_NOTE_3                  :  Event note field 3        
   COR_EVENT_CONS_MER_NUMBER         :  Conversion/merger number        
   COR_EVENT_COR_NAME                :  Corporation name         
   COR_EVENT_NAME_SEQ                :  Event corporation name sequence        
   COR_EVENT_X_NAME_SEQ              :  Event cross reference name sequence        
   COR_EVENT_NAME_CHG                :  Event name change : 
                                                            "Y" (If event changes cor name)
                                                            " " (If no changes or made to name)        
   COR_EVENT_X_NAME_CHG              :  Cross reference name change :
                                                             "Y" (If event changes cor name)
                                                             " " (If no changes or made to name)        
   COR_EVENT_ADD_1                   :  1st line of corporate address                  
   COR_EVENT_ADD_2                   :  2nd line of corporate address   
   COR_EVENT_CITY                    :  Corporate city   
   COR_EVENT_STATE                   :  Corporate state   
   COR_EVENT_ZIP                     :  Corporate zip   
   COR_EVENT_MAIL_ADD_1              :  1st line of corporate mailing address    
   COR_EVENT_MAIL_ADD_2              :  1st line of corporate mailing address    
   COR_EVENT_MAIL_CITY               :  Mail city    
   COR_EVENT_MAIL_STATE              :  Mail state    
   COR_EVENT_MAIL_ZIP                :  Mail zip    

## Source: sunbiz_FLRreadme.txt

        		QUARTERLY FLR FILINGS


	 
 

        These files contain the Federal Lien Registrations (FLR)  data from the 
	Florida Secretary of State.


        Included in this README.TXT is the file description of
        the data file, along with a brief description of each data
        element.



	The lien data base consists of four different data files.

	1).  The filing record (LIEN_DATA_FILE)    		flrf.txt

	2).  The Current Debtors (LIEN_DEB_DATA_FILE)		flrd.txt

	3).  The Current Secureds (LIEN_SEC_DATA_FILE)		flrs.txt

	4).  Events and actions					flre.txt


   FD  LIEN_DATA_FILE
    BLOCK CONTAINS 250 RECORDS
    RECORD CONTAINS 82 CHARACTERS
    DATA RECORD IS LIEN_DATA_REC.

01 LIEN_DATA_REC.
    03 LIEN_DATA_DOC_NUMBER                       PIC X(12).
    03 LIEN_DATA_FILING_DATE                      PIC X(08).
    03 LIEN_DATA_PAGES                            PIC S9(05).
    03 LIEN_DATA_TOTAL_PAGES                      PIC S9(05).
    03 LIEN_DATA_FILING_STATUS                    PIC X(01).
    03 LIEN_DATA_FILING_TYPE                      PIC X(01).
    03 LIEN_DATA_ASSESSMENT_DATE                  PIC X(08).
    03 LIEN_DATA_CANCELLATION_DATE                PIC X(08).
    03 LIEN_DATA_EXPIRATION_DATE                  PIC X(08).
    03 LIEN_DATA_TRANS_UTILITY                    PIC X(01).
    03 LIEN_DATA_FILING_EVENT_COUNT               PIC 9(05).
    03 LIEN_DATA_FILING_TOTAL_DEB_CTR             PIC 9(05).
    03 LIEN_DATA_FILING_TOTAL_SEC_CTR             PIC 9(05).
    03 LIEN_DATA_FILING_CUR_DEB_CTR               PIC 9(05).
    03 LIEN_DATA_FILING_CUR_SEC_CTR               PIC 9(05).


 The following is an explanation of what each field contains.


    LIEN_DATA_DOC_NUMBER           :  The Lien document number  (filing number) 
    LIEN_DATA_FILING_DATE          :  Date filed with our office
    LIEN_DATA_PAGES                :  Number of pages filed 
    LIEN_DATA_TOTAL_PAGES          :  Total number of pages filed 
    LIEN_DATA_FILING_STATUS        :  Status (Terminated (T) Lapsed (L) and 
                                      Active (A)  
    LIEN_DATA_FILING_TYPE          :  Filing type LIEN (U) or FLR (F)   
    LIEN_DATA_ASSESSMENT_DATE      :  DATE FOR FLR'S  
    LIEN_DATA_CANCELLATION_DATE    :  Date filing was cancelled  
    LIEN_DATA_EXPIRATION_DATE      :  Date filing will expire  
    LIEN_DATA_TRANS_UTILITY        :  Transmitting Utility (Y?N)  
    LIEN_DATA_FILING_EVENT_COUNT   :  Total events filed against filing 
    LIEN_DATA_FILING_TOTAL_DEB_CTR :  Total number of debtors associated with filing 
    LIEN_DATA_FILING_TOTAL_SEC_CTR :  Total number of secureds associated with filing    
    LIEN_DATA_FILING_CUR_DEB_CTR   :  Number of debtors associated with filing  
    LIEN_DATA_FILING_CUR_SEC_CTR   :  Number of secured associated with filing  


   FD  LIEN_DEB_DATA_FILE
    RECORD CONTAINS 206 CHARACTERS
    DATA RECORD IS LIEN_DEB_DATA_REC.

01  LIEN_DEB_DATA_REC.
    03 LIEN_DEB_DATA_FILING_TYPE                 PIC X(01).
    03 LIEN_DEB_DATA_DOC_NUMBER                  PIC X(12).
    03 LIEN_DEB_DATA_NAME                        PIC X(55).
    03 LIEN_DEB_DATA_NAME_FORMAT                 PIC X(01).
    03 LIEN_DEB_DATA_ADDRESS1                    PIC X(44).
    03 LIEN_DEB_DATA_ADDRESS2                    PIC X(44).
    03 LIEN_DEB_DATA_CITY                        PIC X(28).
    03 LIEN_DEB_DATA_STATE                       PIC X(02).
    03 LIEN_DEB_DATA_ZIP_CODE                    PIC X(09).
    03 LIEN_DEB_DATA_COUNTRY                     PIC X(02).
    03 LIEN_DEB_DATA_SEQ_CTR                     PIC 9(05).
    03 LIEN_DEB_DATA_REL_TO_FILING               PIC X(01).
    03 LIEN_DEB_DATA_ORIG_PARTY                  PIC X(01).
    03 LIEN_DEB_DATA_FILING_STATUS               PIC X(01).
    


   The following is an explanation of what each field contains.
    this breakdown holds true for the Secured record as well.
                                                        
    LIEN_DEB_DATA_FILING_TYPE     :  Type of filing 
    LIEN_DEB_DATA_DOC_NUMBER      :  Filing document number  
    LIEN_DEB_DATA_NAME            :  Debtor Name
    LIEN_DEB_DATA_NAME_FORMAT     :  Name format (C) corporate (P) personal
    LIEN_DEB_DATA_ADDRESS_LINE1   :  1st line of address
    LIEN_DEB_DATA_ADDRESS_LINE2   :  2nd line of address
    LIEN_DEB_DATA_CITY            :  City
    LIEN_DEB_DATA_STATE           :  State
    LIEN_DEB_DATA_ZIP_CODE        :  Zip
    LIEN_DEB_DATA_COUNTRY         :  Country
    LIEN_DEB_DATA_SEQ_CTR         :  Sequence number of debtor    
    LIEN_DEB_DATA_REL_TO_FILING   :  Relation to filing (C) current
    LIEN_DEB_DATA_ORIG_PARTY      :  Original debtor        
    LIEN_DEB_DATA_FILING_STATUS   :  Filing status (A) active (L) lapsed (T) terminated    


  FD  LIEN_SEC_DATA_FILE
    RECORD CONTAINS 206 CHARACTERS
    DATA RECORD IS LIEN_SEC_DATA_REC.

01  LIEN_SEC_DATA_REC.
    03 LIEN_SEC_DATA_FILING_TYPE                 PIC X(01).
    03 LIEN_SEC_DATA_DOC_NUMBER                  PIC X(12).
    03 LIEN_SEC_DATA_NAME                        PIC X(55).
    03 LIEN_SEC_DATA_NAME_FORMAT                 PIC X(01).
    03 LIEN_SEC_DATA_ADDRESS1                    PIC X(44).
    03 LIEN_SEC_DATA_ADDRESS2                    PIC X(44).
    03 LIEN_SEC_DATA_CITY                        PIC X(28).
    03 LIEN_SEC_DATA_STATE                       PIC X(02).
    03 LIEN_SEC_DATA_ZIP_CODE                    PIC X(09).
    03 LIEN_SEC_DATA_COUNTRY                     PIC X(02).
    03 LIEN_SEC_DATA_SEQ_CTR                     PIC 9(05).
    03 LIEN_SEC_DATA_REL_TO_FILING               PIC X(01).
    03 LIEN_SEC_DATA_ORIG_PARTY                  PIC X(01).
    03 LIEN_SEC_DATA_FILING_STATUS               PIC X(01).





FD  LIEN_EVENT_AND_ACTION_FILE
    BLOCK CONTAINS 250 RECORDS
    RECORD CONTAINS 320 CHARACTERS
    DATA RECORD IS LIEN_EVENT_ACT_DATA_REC.

01  LIEN_EVENT_ACT_DATA_REC.                         
   03  EVENT_DOC_NUMBER                      PIC X(12).
   03  EVENT_ORIG_DOC_NUMBER                 PIC X(12).
   03  EVENT_ACTION_CTR                      PIC 9(05).
   03  EVENT_SEQ_NUMBER                      PIC 9(05).
   03  EVENT_PAGES                           PIC 9(05).
   03  EVENT_DATE                            PIC X(08).
   03  ACTION_SEQ_NUMBER                     PIC 9(05).
   03  ACTION_CODE                           PIC X(03).
   03  ACTION_VERBAGE                        PIC X(70).
   03  ACTION_NAME                           PIC X(55).
   03  ACTION_ADDRESS_LINE1                  PIC X(44).
   03  ACTION_ADDRESS_LINE2                  PIC X(44).
   03  ACTION_CITY                           PIC X(28).
   03  ACTION_STATE                          PIC X(02).
   03  ACTION_ZIP                            PIC X(09).
   03  ACTION_COUNTRY                        PIC X(02).
   03  ACTION_OLD_NAME_SEQ                   PIC 9(05).
   03  ACTION_NEW_NAME_SEQ                   PIC 9(05).
   03  ACTION_NAME_TYPE                      PIC X(01). 

The following is a brief description of what each field contains.

EVENT_DOC_NUMBER               : Number assigned to event document       
EVENT_ORIG_DOC_NUMBER          : Number assigned to original document       
EVENT_ACTION_CTR               : Number of actions event consisted of       
EVENT_SEQ_NUMBER               : Event sequence      
EVENT_PAGES                    : Number of pages event form contained      
EVENT_DATE                     : Date event was filed      
ACTION_SEQ_NUMBER              : action sequence     
ACTION_CODE                    : action code :  A   (AMENDMENT)                 
                                                ADD (Add AN ENTITY)               
                                                ADS (AMEND DOCUMENTARY STAMP NOTATION)               
                                                AE  (ACCEPTED IN ERROR)               
                                                AGI (AMEND GENERAL INFORMATION)              
                                                BAN (BANKRUPTCY)               
                                                C   (CONTINUATION)               
                                                CHA (CHANGE ENTITY)               
                                                CN  (CHANGE ENTITY)               
                                                COD (CERTIFICATE OF DISCHARGE)               
                                                COL (AMEND TO COLLATERAL)               
                                                CON (CERTIFICATE OF NONATTACHMENT)               
                                                COR (CERTIFICATE OF RELEASE)               
                                                COS (CERTIFICATE OF SUBORDIATION)               
                                                DM  (FILING CANCELLED)                              
                                                PR  (PARTIAL RELEASE)               
                                                R   (RELEASE)               
                                                REM (REMOVE)               
                                                RNC (NOTICE OF REFILING)               
                                                RNL (REFILED NOTICE OF LIEN)               
                                                SUB (SUBORDINATION AGREEMENT)               
                                                T   (TERMINATION)               
                                                TU  (DEBTOR IS TRANSMITTING UTILITY)
                                                TYP (TYPOGRAPHICAL ERROR)            
ACTION_VERBAGE                 : DESCRIPTION OF EVENT     
ACTION_NAME                    : ENTITY NAME BEING CHANGED OR ADDED 
ACTION_ADDRESS_LINE1           : 1ST LINE OF ENTITY'S ADDRESS                   
ACTION_ADDRESS_LINE2           : 2ND LINE OF ENTITY'S ADDRESS       
ACTION_CITY                    : ENTITY'S CITY       
ACTION_STATE                   : ENTITY'S STATE       
ACTION_ZIP                     : ENTITY'S ZIP      
ACTION_COUNTRY                 : ENTITY'S COUNTRY      
ACTION_OLD_NAME_SEQ            : OLD NAME SEQUENCE       
ACTION_NEW_NAME_SEQ            : NEW NAME SEQUENCE      
ACTION_NAME_TYPE               : NAME TYPE "D" (DEBTOR) OR "S" (SECURED PARTY)        






## Source: sunbiz_Ficreadme.txt


                FICTITIOUS NAME REGISTRATION DATA 


        This disc contains a  data file labeled FICFILE.EXE. This 
        is a self expanding zip file that contains a file called
        ficfile.dat.  By double clicking on the .exe file the unzip program
        will launch and ask you where you would like the file loaded.
        The file size of the expanded file is about 800 megabytes.  This
        file contains the active Fictitious Name Registration data 
	from the Florida Secretary of State.

	There is also a file on the disc called FICEVT.DAT.  This file contains
	the events (changes) that have been filed against the original filings.

        Included in this README.TXT is the file description of
        the data file, along with a brief description of each data
        element.


FD  CREATE_FIC_FILING_PULL
    BLOCK CONTAINS 10 RECORDS
    RECORD CONTAINS 2098 CHARACTERS
    DATA RECORD IS OUT_REC.

01  OUT_REC.
    05  FIC_FIL_DOC_NUM                    PIC X(12).
    05  FIC_FIL_NAME                       PIC X(192).
    05  FIC_FIL_COUNTY                     PIC X(12).
    05  FIC_FIL_ADDR1                      PIC X(40).
    05  FIC_FIL_ADDR2                      PIC X(40).
    05  FIC_FIL_CITY                       PIC X(28).
    05  FIC_FIL_STATE                      PIC X(02).
    05  FIC_FIL_ZIP                        PIC X(10).
    05  FIC_FIL_COUNTRY                    PIC X(02).
    05  FIC_FIL_DATE                       PIC X(08).
    05  FIC_FIL_PAGES                      PIC 9(05).
    05  FIC_FIL_STATUS                     PIC X(01).
    05  FIC_FIL_CANCELLATION_DATE          PIC X(08).
    05  FIC_FIL_EXPIRATION_DATE            PIC X(08).
    05  FIC_FIL_TOTAL_OWN_CUR_CTR          PIC 9(05).
    05  FIC_FIL_FEI_NUM                    PIC X(14).
    05  FIC_GREATER_THAN_10_OWNERS         PIC X(01).
    05  FIC_OWNERS                         OCCURS 10 TIMES.
        10  FIC_OWNER_DOC_NUM              PIC X(12).
        10  FIC_OWNER_NAME                 PIC X(55).
        10  FIC_OWNER_NAME_FORMAT          PIC X(01).
        10  FIC_OWNER_ADDR                 PIC X(40).
        10  FIC_OWNER_CITY                 PIC X(28).
        10  FIC_OWNER_STATE                PIC X(02).
        10  FIC_OWNER_ZIP                  PIC X(10).
        10  FIC_OWNER_COUNTRY              PIC X(02).
        10  FIC_OWNER_FEI_NUM              PIC X(09).
        10  FIC_OWNER_CHARTER_NUM          PIC X(12).


                Data file is FICFILE.DAT



    The following is a brief explanation of what each field contains.

    FIC_FIL_DOC_NUMBER          :   The fic document number (filing number)   
    FIC_FIL_NAME                :   The name of the fictitious filing
    FIC_FIL_COUNTY              :   The county in which it resides
    FIC_FIL_ADDR1               :   1st line of address
    FIC_FIL_ADDR2               :   2nd line of address
    FIC_FIL_CITY                :   city
    FIC_FIL_STATE               :   state
    FIC_FIL_ZIP                 :   zip code
    FIC_FIL_COUNTRY             :   country
    FIC_FIC_FILING_DATE         :   Date filed with our office
    FIC_FIL_PAGES               :   Total number of pages filed
    FIC_FIL_STATUS              :   Status: (C) cancelled (E) Expired  and
                                     (A) Active 
    FIC_DATA_CANCELLATION_DATE  :   Date filing was cancelled
    FIC_DATA_EXPIRATION_DATE    :   Date filing will expire
    FIC_DATA_TOTAL_OWN_CUR_CTR  :   Number of owners associated with filing
    FIC_DATA_FEI_NUMBER         :   FEI number associated with filing
    FIC_GREATER_THAN_10_OWNERS  :   (N) = Less than 10 owners
 				    (Y) = More than 10 owners, call Corporations
                                          for complete list.

    FIC_OWNERS                         This is a table of the owners (up to 10 entries)
     FIC_OWNER_DOC_NUM           :  Filing Document number 
     FIC_OWNER_NAME              :  Owner name   
     FIC_OWNER_NAME_FORMAT       :  Owner name format  (P) person (C) corporation 
     FIC_OWNER_ADDR              :  Owner address   
     FIC_OWNER_CITY              :  Owner city
     FIC_OWNER_STATE             :  Owner state
     FIC_OWNER_ZIP               :  Owner zip
     FIC_OWNER_COUNTRY           :  Owner country
     FIC_OWNER_FEI_NUM           :  Owner FEI number   
     FIC_OWNER_CHARTER_NUM       :  Owner Charter number





FD  FIC_EVENT_AND_ACTION_FILE
    BLOCK CONTAINS 250 RECORDS
    RECORD CONTAINS 762 CHARACTERS
    DATA RECORD IS FIC_EVENT_ACT_DATA_REC.

01  FIC_EVENT_ACT_DATA_REC.                         
    03  EVENT_DOC_NUMBER                      PIC X(12).
    03  EVENT_ORIG_DOC_NUMBER                 PIC X(12).
    03  EVENT_FIC_NAME                        PIC X(192). 
    03  EVENT_ACTION_CTR                      PIC 9(05).
    03  EVENT_SEQ_NUMBER                      PIC 9(05).
    03  EVENT_PAGES                           PIC 9(05).
    03  EVENT_DATE                            PIC X(08).
    03  ACTION_SEQ_NUMBER                     PIC 9(05).
    03  ACTION_CODE                           PIC X(03).
    03  ACTION_VERBAGE                        PIC X(70).
    03  ACTION_OLD_FEI                        PIC X(09).
    03  ACTION_OLD_COUNTY                     PIC X(12).
    03  ACTION_OLD_ADDR1                      PIC X(40).
    03  ACTION_OLD_ADDR2                      PIC X(40). 
    03  ACTION_OLD_CITY                       PIC X(28).
    03  ACTION_OLD_STATE                      PIC X(02).
    03  ACTION_OLD_ZIP5                       PIC X(05).
    03  ACTION_OLD_ZIP4                       PIC X(04).
    03  ACTION_OLD_COUNTRY                    PIC X(02).
    03  ACTION_NEW_FEI                        PIC X(09).
    03  ACTION_NEW_COUNTY                     PIC X(12).
    03  ACTION_NEW_ADDR1                      PIC X(40). 
    03  ACTION_NEW_ADDR2                      PIC X(40).
    03  ACTION_NEW_CITY                       PIC X(28).
    03  ACTION_NEW_STATE                      PIC X(02).
    03  ACTION_NEW_ZIP5                       PIC X(05).
    03  ACTION_NEW_ZIP4                       PIC X(04).   
    03  ACTION_NEW_COUNTRY                    PIC X(02).
    03  ACTION_OWN_NAME                       PIC X(55).
    03  ACTION_OWN_ADDRESS                    PIC X(40).
    03  ACTION_OWN_CITY                       PIC X(28).
    03  ACTION_OWN_STATE                      PIC X(02).
    03  ACTION_OWN_ZIP5                       PIC X(05).
    03  ACTION_OWN_FEI                        PIC X(09).
    03  ACTION_OWN_CHARTER_NUMBER             PIC X(12).
    03  ACTION_OLD_NAME_SEQ                   PIC 9(05).
    03  ACTION_NEW_NAME_SEQ                   PIC 9(05).

The following is a brief explanation of what each field contains.

EVENT_DOC_NUMBER                 : Number assigned to the fic event document  
EVENT_ORIG_DOC_NUMBER            : Number assigned to the original fic document     
EVENT_FIC_NAME                   : Fictitious name
EVENT_ACTION_CTR                 : Number of actions event consisted of     
EVENT_SEQ_NUMBER                 : Event sequence     
EVENT_PAGES                      : Number of pages event document contained     
EVENT_DATE                       : Date event was filed     
ACTION_SEQ_NUMBER                : Action sequence    
ACTION_CODE                      : Action code : ADD (ADD AN OWNER)    
ACTION_VERBAGE                                   AME (AMENDMENT)                         
                                                 CAN (CANCELLATION) 
                                                 CHF (CHANGE FIC ADDRESS 
                                                 CHO (CHANGE FIC OWNER)  
                                                 DEL (DELETE AN OWNER)

Fictitious filings old and new address and FEI number.
ACTION_OLD_FEI                   : Old FEI number     
ACTION_OLD_COUNTY                : Old county in which Fic resided     
ACTION_OLD_ADDR1                 : 1st line of old address  
ACTION_OLD_ADDR2                 : 2nd line of old address    
ACTION_OLD_CITY                  : Old city   
ACTION_OLD_STATE                 : Old state    
ACTION_OLD_ZIP5                  : Old 5 digit zip   
ACTION_OLD_ZIP4                  : Old 4 digit zip ext.     
ACTION_OLD_COUNTRY               : Old country  
ACTION_NEW_FEI                   : New FEI number     
ACTION_NEW_COUNTY                : New county in which fic resides   
ACTION_NEW_ADDR1                 : 1st line of new address      
ACTION_NEW_ADDR2                 : 2nd line of new address  
ACTION_NEW_CITY                  : New city   
ACTION_NEW_STATE                 : New state     
ACTION_NEW_ZIP5                  : New 5 digit zip    
ACTION_NEW_ZIP4                  : New 4 digit zip ext.     
ACTION_NEW_COUNTRY               : New country 

Owner's name and address.
ACTION_OWNER_NAME                : Owner name                 
ACTION_OWN_ADDRESS               : Owner address                
ACTION_OWN_CITY                  : Owner city                
ACTION_OWN_STATE                 : Owner state                
ACTION_OWN_ZIP5                  : Owner zip              
ACTION_OWN_FEI                   : Owner FEI number     
ACTION_OWN_CHARTER_NUMBER        : Charter number for owner (If owner is not an individual)    
ACTION_OLD_NAME_SEQ              : Owner old name sequence     
ACTION_NEW_NAME_SEQ              : Owner new name sequence 

## Source: sunbiz_cor_99-README.TXT

For Events, click on "Events" rather than selecting from this list.
For current filings, selecting from this list will work.


Some older records may be found at:

ftp://ftp.dos.state.fl.us/pub/doc/


## Source: sunbiz_cor_Filings_README.TXT

Some older records may be found at:

ftp://ftp.dos.state.fl.us/pub/doc/


## Source: sunbiz_cor_README.TXT

Some older records may be found at:

ftp://ftp.dos.state.fl.us/pub/doc/


## Source: sunbiz_corindex.txt

Filings
Events
99-README.TXT
120902c.dat
120602c.dat
120502c.dat
120402c.dat
120302c.dat
120202c.dat
112702c.dat
112602c.dat
112502c.dat
112202c.dat
112102c.dat
112002c.dat
111902c.dat
111802c.dat
111502c.dat
111402c.dat
111302c.dat
111202c.dat
110802c.dat
110702c.dat
110602c.dat
110502c.dat
110402c.dat
110102c.dat
103102c.dat
103002c.dat
102902c.dat
102802c.dat
102502c.dat
102402c.dat
102302c.dat
102202c.dat
102102c.dat
101802c.dat
101702c.dat
101602c.dat
101502c.dat
101402c.dat
101102c.dat
101002c.dat
100902c.dat
100802c.dat
100702c.dat
100402c.dat
100302c.dat
100202c.dat
100102c.dat
093002c.dat
092702c.dat
092602c.dat
092502c.dat
092402c.dat
092302c.dat
092002c.dat
091902c.dat
091802c.dat
091702c.dat
091602c.dat
091302c.dat
091202c.dat
091102c.dat
091002c.dat
090902c.dat
090602c.dat
090502c.dat
090402c.dat
090302c.dat
083002c.dat
082902c.dat
082802c.dat
082702c.dat
082602c.dat
082302c.dat
082202c.dat
082102c.dat
082002c.dat
081902c.dat
081602c.dat
081502c.dat
081402c.dat
081302c.dat
081202c.dat
080902c.dat
080802c.dat
080702c.dat
080602c.dat
080502c.dat
080202c.dat
080102c.dat
073102c.dat
073002c.dat
072902c.dat
072602c.dat
072502c.dat
072402c.dat
072302c.dat
072202c.dat
071902c.dat
071802c.dat
071702c.dat
071602c.dat
071502c.dat
071202c.dat
071102c.dat
071002c.dat
070902c.dat
070802c.dat
070502c.dat
070302c.dat
070202c.dat
070102c.dat
062802c.dat
062702c.dat
062602c.dat
062502c.dat
062402c.dat
062102c.dat
062002c.dat
061902c.dat
061802c.dat
061702c.dat
061402c.dat
061302c.dat
061202c.dat
061102c.dat
061002c.dat
060702c.dat
060602c.dat
060502c.dat
060402c.dat
060302c.dat
053102c.dat
053002c.dat
052902ce.dat
052902c.dat
052802c.dat
052402c.dat
052302c.dat
052202c.dat
052102c.dat
052002c.dat
051702c.dat
051602c.dat
051502c.dat
051402c.dat
051302c.dat
051002c.dat
050902c.dat
050802c.dat
050702c.dat
050602c.dat
050302c.dat
050202c.dat
050102c.dat
043002c.dat
042902c.dat
042602c.dat
042502c.dat
042402c.dat
042302c.dat
042202c.dat
041902c.dat
041802c.dat
041702c.dat
041602c.dat
041502c.dat
041202c.dat
041102c.dat
041002c.dat
040902c.dat
040802c.dat
040502c.dat
040402c.dat
040302c.dat
040202c.dat
040102c.dat
032902c.dat
032802c.dat
032702c.dat
032602c.dat
032502c.dat
032202c.dat
032102c.dat
032002c.dat
031902c.dat
031802c.dat
031502c.dat
031402c.dat
031302c.dat
031202c.dat
031102c.dat
030802c.dat
030702c.dat
030602c.dat
030502c.dat
030402c.dat
030102c.dat
022802c.dat
022702c.dat
022602c.dat
022502c.dat
022202c.dat
022102c.dat
022002c.dat
021902c.dat
021802c.dat
021502c.dat
021402c.dat
021302c.dat
021202c.dat
021102c.dat
020802c.dat
020702c.dat
020602c.dat
020502c.dat
020402c.dat
020102c.dat
013102c.dat
013002c.dat
012902c.dat
012802c.dat
012502c.dat
012402c.dat
012302c.dat
012202c.dat
011802c.dat
011702c.dat
011602c.dat
011502c.dat
011402c.dat
011102c.dat
011002c.dat
010902c.dat
010802c.dat
010702c.dat
010402c.dat
010302c.dat
010202c.dat
README.TXT
120902ce.dat
120602ce.dat
120502ce.dat
120402ce.dat
120302ce.dat
120202ce.dat
112702ce.dat
112602ce.dat
112502ce.dat
112202ce.dat
112102ce.dat
112002ce.dat
111902ce.dat
111802ce.dat
111502ce.dat
111402ce.dat
111302ce.dat
111202ce.dat
110802ce.dat
110702ce.dat
110602ce.dat
110502ce.dat
110402ce.dat
110102ce.dat
103102ce.dat
103002ce.dat
102902ce.dat
102802ce.dat
102502ce.dat
102402ce.dat
102302ce.dat
102202ce.dat
102102ce.dat
101802ce.dat
101702ce.dat
101602ce.dat
101502ce.dat
101402ce.dat
101102ce.dat
101002ce.dat
100902ce.dat
100802ce.dat
100702ce.dat
100402ce.dat
100302ce.dat
100202ce.dat
100102ce.dat
093002ce.dat
092702ce.dat
092602ce.dat
092502ce.dat
092402ce.dat
092302ce.dat
092002ce.dat
091902ce.dat
091802ce.dat
091702ce.dat
091602ce.dat
091302ce.dat
091202ce.dat
091102ce.dat
091002ce.dat
090902ce.dat
090602ce.dat
090502ce.dat
090402ce.dat
090302ce.dat
083002ce.dat
082902ce.dat
082802ce.dat
082702ce.dat
082602ce.dat
082302ce.dat
082202ce.dat
082102ce.dat
082002ce.dat
081902ce.dat
081602ce.dat
081502ce.dat
081402ce.dat
081302ce.dat
081202ce.dat
080902ce.dat
080802ce.dat
080702ce.dat
080602ce.dat
080502ce.dat
080202ce.dat
080102ce.dat
073102ce.dat
073002ce.dat
072902ce.dat
072602ce.dat
072502ce.dat
072402ce.dat
072302ce.dat
072202ce.dat
071902ce.dat
071802ce.dat
071702ce.dat
071602ce.dat
071502ce.dat
071202ce.dat
071102ce.dat
071002ce.dat
070902ce.dat
070802ce.dat
070502ce.dat
070302ce.dat
070202ce.dat
070102ce.dat
062802ce.dat
062702ce.dat
062602ce.dat
062502ce.dat
062402ce.dat
062102ce.dat
062002ce.dat
061902ce.dat
061802ce.dat
061702ce.dat
061402ce.dat
061302ce.dat
061202ce.dat
061102ce.dat
061002ce.dat
060702ce.dat
060602ce.dat
060502ce.dat
060402ce.dat
060302ce.dat
053102ce.dat
053002ce.dat
052902ce.dat
052802ce.dat
052402ce.dat
052302ce.dat
052202ce.dat
052102ce.dat
052002ce.dat
051702ce.dat
051602ce.dat
051502ce.dat
051402ce.dat
051302ce.dat
051002ce.dat
050902ce.dat
050802ce.dat
050702ce.dat
050602ce.dat
050302ce.dat
050202ce.dat
050102ce.dat
043002ce.dat
042902ce.dat
042602ce.dat
042502ce.dat
042402ce.dat
042302ce.dat
042202ce.dat
041902ce.dat
041802ce.dat
041702ce.dat
041602ce.dat
041502ce.dat
041202ce.dat
041102ce.dat
041002ce.dat
040902ce.dat
040802ce.dat
040502ce.dat
040402ce.dat
040302ce.dat
040202ce.dat
040102ce.dat
032902ce.dat
032802ce.dat
032702ce.dat
032602ce.dat
032502ce.dat
032202ce.dat
032102ce.dat
032002ce.dat
031902ce.dat
031802ce.dat
031502ce.dat
031402ce.dat
031302ce.dat
031202ce.dat
031102ce.dat
030802ce.dat
030702ce.dat
030602ce.dat
030502ce.dat
030402ce.dat
030102ce.dat
022802ce.dat
022702ce.dat
022602ce.dat
022502ce.dat
022202ce.dat
022102ce.dat
022002ce.dat
021902ce.dat
021802ce.dat
021502ce.dat
021402ce.dat
021302ce.dat
021202ce.dat
021102ce.dat
020802ce.dat
020702ce.dat
020602ce.dat
020502ce.dat
020402ce.dat
020102ce.dat
013102ce.dat
013002ce.dat
012902ce.dat
012802ce.dat
012502ce.dat
012402ce.dat
012302ce.dat
012202ce.dat
011802ce.dat
011702ce.dat
011602ce.dat
011502ce.dat
011402ce.dat
011102ce.dat
011002ce.dat
010902ce.dat
010802ce.dat
010702ce.dat
010402ce.dat
010302ce.dat
010202ce.dat
010102ce.dat


## Source: sunbiz_cornp_readme.txt

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


     

## Source: sunbiz_entity_file_layout.md

# Sunbiz Entity File Layout (COR + GEN)

Last verified: February 19, 2026.

## Official Sources

- Data Usage Guide: https://dos.fl.gov/sunbiz/other-services/data-downloads/data-usage-guide/
- Corporate definitions: https://dos.sunbiz.org/data-definitions/cor.html
- General partnership definitions: https://dos.sunbiz.org/data-definitions/gen.html

## Files Needed for LLCs, Partnerships, Companies

Quarterly baseline:
- `/public/doc/quarterly/Cor/cordata.zip`
- `/public/doc/quarterly/Cor/corevt.zip`
- `/public/doc/quarterly/Gen/Genfile.zip`
- `/public/doc/quarterly/Gen/Genevt.zip`
- `/public/doc/quarterly/Non-Profit/npcordata.zip` (optional if you also want nonprofit entities)

## Record Lengths

- Corporate Data (`cordata*.txt`): `1440`
- Corporate Events (`corevt.txt`): `662`
- General Partnership Data (`GENFILE.TXT`): `759`
- General Partnership Events (`GENEVT.TXT`): `910`

## Key Columns Used by Loader

### Corporate Data (selected)
- `doc_number`: start `1`, len `12`
- `entity_name`: start `13`, len `192`
- `status`: start `205`, len `1`
- `filing_type`: start `206`, len `15`
- principal address block: starts `221`
- mailing address block: starts `347`
- `filed_date`: start `473`, len `8`
- `fei_number`: start `481`, len `14`
- officer slots (6): start `669`, block size `128`

### Corporate Events (selected)
- `event_doc_number`: `1/12`
- `event_sequence`: `13/5`
- `event_code`: `18/20`
- `event_description`: `38/40`
- `event_effective_date`: `78/8`
- `event_filing_date`: `86/8`
- `event_name`: `211/192`

### General Partnership Data (selected)
- `doc_number`: `1/12`
- `status`: `13/1`
- `entity_name`: `14/192`
- `filed_date`: `206/8`
- `effective_date`: `214/8`
- `cancellation_date`: `222/8`
- `expiration_date`: `752/8`
- partner fields: start near `501` (`name` at `515/55`, `seq` at `570/5`)

### General Partnership Events (selected)
- `event_doc_number`: `1/12`
- `event_orig_doc_number`: `13/12`
- `event_sequence`: `25/5`
- `event_code`: `30/20`
- `event_description`: `50/40`
- `event_effective_date`: `95/8`
- `event_filing_date`: `103/8`
- `event_name`: `249/192`

## Database Strategy

Use multiple tables:
- `sunbiz_entity_filings` (1 row per entity filing doc)
- `sunbiz_entity_parties` (officers/partners)
- `sunbiz_entity_events` (event timeline)

This supports clean upserts, avoids denormalized duplication, and allows a separate materialized "current snapshot" later for fast UI reads.
