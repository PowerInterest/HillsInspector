

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