# HOW TO RUN THE SCRIPT
Change directory to root where we could find Makefile,Dockerfile,data folder, solution folder and so on. Then command in Makefile to run specific process such as export requirements (required poetry installation), running script, etc

IN EXAMPLE:
1. run the script and show the result to terminal : type "make local-run" in terminal
2. build script in the container : type "make docker-build"

OR you can copy the command in Makefile and execute it with your preferences/settings.

*make command only works on Linux since I made the code in ubuntu so you can edit it as you need.


# DESIGN/SOLUTION THINKING
Since the data in json file, I need to find the way to load it into my data structure which is dictionary (I choose this due to flexibility and information attachment capability). The data contains Change Data Capture of 1 transaction account which has 2 cards and 1 saving. I need to break down those table and make sure the data in CDC record has been distributed well as columns by doing value extracting, data stacking, and null filling. After that, I tried to join those 3 tables based on common columns (card_id and savings_account_id). I get connected information tables with their own respective columns. Last, I need to idenfity transaction data. Since the data has been merged, I need to extract between update event from whole data and filter it with several condition like availability of balance and credit value, timestamp, and so on. I do this because update event represent transaction (change in balance and credit). I conclude that there are 8 transactions (you could see this on 2 last tables showed after running script). I focused on credit_used and balance column since it represent current value or change. Each of them has different value and event timestamp. Also I added balance and credit_used into total transaction (transaction_val) to make the identification easier.  

(This is the copy of transaction tables showed in terminal)
op refers to op savings, not account

Transaction Tables
   id_account     id_card           id op_card   op  credit_used  balance  transaction_val      ts_transaction
0  a1globalid         NaN  sa1globalid     NaN    u          NaN  15000.0          15000.0 2020-01-02 09:00:00
1  a1globalid  c1globalid          NaN       u  NaN      12000.0      NaN          12000.0 2020-01-06 12:30:00
2  a1globalid  c1globalid          NaN       u  NaN      19000.0      NaN          19000.0 2020-01-07 18:00:00
3  a1globalid         NaN  sa1globalid     NaN    u          NaN  40000.0          40000.0 2020-01-10 09:30:00
4  a1globalid         NaN  sa1globalid     NaN    u          NaN  21000.0          21000.0 2020-01-10 11:00:00
5  a1globalid  c1globalid          NaN       u  NaN          0.0      NaN              0.0 2020-01-10 11:00:00
6  a1globalid  c2globalid          NaN       u  NaN      37000.0      NaN          37000.0 2020-01-18 15:30:00
7  a1globalid         NaN  sa1globalid     NaN    u          NaN  33000.0          33000.0 2020-01-20 07:30:00
       ts_transaction  transaction_count
0 2020-01-02 09:00:00                  1
1 2020-01-06 12:30:00                  1
2 2020-01-07 18:00:00                  1
3 2020-01-10 09:30:00                  1
4 2020-01-10 11:00:00                  2
5 2020-01-18 15:30:00                  1
6 2020-01-20 07:30:00                  1