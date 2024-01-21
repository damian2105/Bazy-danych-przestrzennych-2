#!/bin/bash

# Changelog:
# - Skrypt automatyzuje proces pobierania, walidacji, przetwarzania i eksportu danych z pliku InternetSales_new.zip
# - Wersja: 1.0
# - Data utworzenia: $(date +"%Y-%m-%d")

# Ustawienia
INDEX_NUMBER="304187"  
LOG_FILE="PROCESSED/cw10_$(date +"%Y%m%d%H%M%S").log"
TIMESTAMP="$(date +"%Y%m%d%H%M%S")"
URL="http://home.agh.edu.pl/~wsarlej/dyd/bdp2/materialy/cw10/InternetSales_new.zip"
ZIP_FILE="InternetSales_new.zip"
PASSWORD="bdp2agh"
DB_USER="root"
DB_PASSWORD="root"
DB_NAME="dmrenca"
SQL_HOST="localhost"
SQL_PORT="3306"
ENCODED_DB_PASSWORD=$(echo -n "$DB_PASSWORD" | base64)

# Krok 0: Sprawdź i stwórz katalog PROCESSED
[ -d "PROCESSED" ] || mkdir "PROCESSED"

# Utwórz plik konfiguracyjny MySQL
MYSQL_CONFIG_FILE=$(mktemp)
echo "[client]" >> "$MYSQL_CONFIG_FILE"
echo "user=$DB_USER" >> "$MYSQL_CONFIG_FILE"
echo "password=$DB_PASSWORD" >> "$MYSQL_CONFIG_FILE"

# Krok a: Pobierz plik
echo "$(date +"%Y%m%d%H%M%S") - Download Step - Started" >> "$LOG_FILE"
wget "$URL" -O "$ZIP_FILE" 2>&1 | tee -a "$LOG_FILE"
echo "$(date +"%Y%m%d%H%M%S") - Download Step - Successful" >> "$LOG_FILE"

# Krok b: Rozpakuj plik
echo "$(date +"%Y%m%d%H%M%S") - Unzip Step - Started" >> "$LOG_FILE" 
unzip -P "$PASSWORD" "$ZIP_FILE" 2>&1 | tee -a "$LOG_FILE"
echo "$(date +"%Y%m%d%H%M%S") - Unzip Step - Successful" >> "$LOG_FILE"

# Krok c: Sprawdź poprawność pliku
echo "$(date +"%Y%m%d%H%M%S") - Validating and transforming file..." >> "$LOG_FILE"
awk -F'|' 'BEGIN {
    OFS="|";
    print "FIRST_NAME|LAST_NAME|ProductKey|CurrencyAlternateKey|OrderDateKey|OrderQuantity|UnitPrice|SecretCode";
}
NF==7 && $5 <= 100 && $6 != "" {
    gsub(/"/, "", $3);  
    split($3, names, ",");
    print names[2], names[1], $1, $2, $4, $5, $6, $7;
}' InternetSales_new.txt > "InternetSales_new.valid_${TIMESTAMP}.txt" \
    2> "InternetSales_new.bad_${TIMESTAMP}.txt"
echo "$(date +"%Y%m%d%H%M%S") - Validation Step - Successful" >> "$LOG_FILE"

# Krok c: Sprawdź poprawność pliku
echo "$(date +"%Y%m%d%H%M%S") - Validating and transforming file..." >> "$LOG_FILE"
awk -F'|' 'BEGIN {
    OFS="|";
    print "FIRST_NAME|LAST_NAME|ProductKey|CurrencyAlternateKey|OrderDateKey|OrderQuantity|UnitPrice|SecretCode";
}
NF==7 && $5 <= 100 && $6 != "" {
    gsub(/"/, "", $3);  
    split($3, names, ",");
    print names[2], names[1], $1, $2, $4, $5, $6, $7;
}' InternetSales_new.txt > "InternetSales_new.valid_${TIMESTAMP}.txt" \
    2> "InternetSales_new.bad_${TIMESTAMP}.txt"
echo "$(date +"%Y%m%d%H%M%S") - Validation Step - Successful" >> "$LOG_FILE"

# Krok d: Utwórz tabelę w bazie MySQL
echo "$(date +"%Y%m%d%H%M%S") - MySQL Table Creation Step - Started" >> "$LOG_FILE"
TABLE_DEFINITION="CREATE TABLE IF NOT EXISTS ${DB_NAME}.CUSTOMERS_${INDEX_NUMBER} (
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    ProductKey INT,
    CurrencyAlternateKey VARCHAR(255),
    OrderDateKey INT,
    OrderQuantity INT,
    UnitPrice FLOAT,
    SecretCode VARCHAR(255)
);"

echo "$(date +"%Y%m%d%H%M%S") - MySQL Table Creation Step - Started" >> "$LOG_FILE"
echo "Table Definition:"
echo "$TABLE_DEFINITION"
mysql --defaults-extra-file="$MYSQL_CONFIG_FILE" -h "$SQL_HOST" -P "$SQL_PORT" --local-infile=1 -e "$TABLE_DEFINITION" > /dev/null 2>&1


CREATE_TABLE_STATUS=$?
if [ $CREATE_TABLE_STATUS -eq 0 ]; then
    echo "$(date +"%Y%m%d%H%M%S") - MySQL Table Creation Step - Successful" >> "$LOG_FILE"
else
    echo "$(date +"%Y%m%d%H%M%S") - MySQL Table Creation Step - Failed." >> "$LOG_FILE"
    exit 1
fi


# Krok e: Załaduj dane do tabeli
echo "$(date +"%Y%m%d%H%M%S") - MySQL Data Load Step - Started" >> "$LOG_FILE"
mysql --defaults-extra-file="$MYSQL_CONFIG_FILE" -h "$SQL_HOST" -P "$SQL_PORT" --local-infile=1  -e "LOAD DATA LOCAL INFILE 'InternetSales_new.valid_${TIMESTAMP}.txt' INTO TABLE ${DB_NAME}.CUSTOMERS_${INDEX_NUMBER} FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES  (FIRST_NAME, LAST_NAME, ProductKey, CurrencyAlternateKey, OrderDateKey, OrderQuantity, UnitPrice, SecretCode);" > /dev/null 2>&1



LOAD_DATA_STATUS=$?
if [ $LOAD_DATA_STATUS -eq 0 ]; then
    echo "$(date +"%Y%m%d%H%M%S") - MySQL Data Load Step - Successful" >> "$LOG_FILE"
else
    echo "$(date +"%Y%m%d%H%M%S") - MySQL Data Load Step - Failed. Aborting further steps." >> "$LOG_FILE"
    exit 1
fi

# Krok f: Przenieś przetworzony plik
echo "$(date +"%Y%m%d%H%M%S") - Move File Step - Started" >> "$LOG_FILE"
mv "InternetSales_new.valid_${TIMESTAMP}.txt" "PROCESSED/${TIMESTAMP}_InternetSales_new.valid.txt"
echo "$(date +"%Y%m%d%H%M%S") - Move File Step - Successful" >> "$LOG_FILE"

# Krok g: Zaktualizuj kolumnę SecretCode
echo "$(date +"%Y%m%d%H%M%S") - Update SecretCode Step - Started" >> "$LOG_FILE"
mysql --defaults-extra-file="$MYSQL_CONFIG_FILE" -h "$SQL_HOST" -P "$SQL_PORT" -e "UPDATE CUSTOMERS_${INDEX_NUMBER} SET SecretCode = SUBSTRING(MD5(RAND()) FROM 1 FOR 10);" "$DB_NAME" 2>&1 | tee -a "$LOG_FILE"
echo "$(date +"%Y%m%d%H%M%S") - Update SecretCode Step - Successful" >> "$LOG_FILE"

# Krok h: Eksportuj dane do pliku csv
echo "$(date +"%Y%m%d%H%M%S") - Export to csv Step - Started" >> "$LOG_FILE"
[ -d "CUSTOMERS_${INDEX_NUMBER}.csv" ] || touch "CUSTOMERS_${INDEX_NUMBER}.csv"

CSV_FILE="CUSTOMERS_${INDEX_NUMBER}.csv"


  export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  mysql --defaults-extra-file="$MYSQL_CONFIG_FILE" -h "$SQL_HOST" -e "SELECT * FROM ${DB_NAME}.CUSTOMERS_${INDEX_NUMBER}" >> "${SCRIPT_DIR}/CUSTOMERS_${INDEX_NUMBER}.csv" --batch --raw --skip-column-names

    EXPORT_CSV_STATUS=$?
    if [ $EXPORT_CSV_STATUS -eq 0 ]; then
        echo "$(date +"%Y%m%d%H%M%S") - Export to CSV Step - Successful" >> "$LOG_FILE"
    else
        echo "$(date +"%Y%m%d%H%M%S") - Export to CSV Step - Failed. MySQL export command returned $EXPORT_CSV_STATUS" >> "$LOG_FILE"
        exit 1
    fi


# Krok i: Skompresuj plik TXT
echo "$(date +"%Y%m%d%H%M%S") - Compression Step - Started" >> "$LOG_FILE"

CSV_FILE="CUSTOMERS_${INDEX_NUMBER}.csv"
GZIP_FILE="CUSTOMERS_${INDEX_NUMBER}.csv.gz"

# Sprawdź, czy plik CSV istnieje i nie jest pusty
if [ -e "$CSV_FILE" ] && [ -s "$CSV_FILE" ]; then
    gzip -c "$CSV_FILE" > "$GZIP_FILE"

    GZIP_STATUS=$?
    if [ $GZIP_STATUS -eq 0 ]; then
        echo "$(date +"%Y%m%d%H%M%S") - Compression Step - Successful" >> "$LOG_FILE"
    else
        echo "$(date +"%Y%m%d%H%M%S") - Compression Step - Failed. Aborting further steps." >> "$LOG_FILE"
        exit 1
    fi
else
    echo "$(date +"%Y%m%d%H%M%S") - Compression Step - Failed. CSV file does not exist or is empty. Aborting further steps." >> "$LOG_FILE"
    exit 1
fi
echo "$(date +"%Y%m%d%H%M%S") - Compression Step - Successful" >> "$LOG_FILE"

# Krok j: Sprawdź i zaloguj
echo "$(date +"%Y%m%d%H%M%S") - Final Check - All Steps Completed Successfully" | tee -a "$LOG_FILE" >> "$LOG_FILE"
echo "$(date +"%Y%m%d%H%M%S") - Final Check - All Steps Completed Successfully" 

# Usuń plik konfiguracyjny MySQL
rm -f "$MYSQL_CONFIG_FILE"

