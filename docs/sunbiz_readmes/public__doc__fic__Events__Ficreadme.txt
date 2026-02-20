
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