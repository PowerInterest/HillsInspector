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




