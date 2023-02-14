# ADD TEST DATA INTO BIGQUERY WITH DATASET CALLED HUBSPOT_COPY

```
for d in * ; 
do    
    FILE_NAME="$d"    
    TABLENAME="${d%_data.*}"    
    echo $FILE_NAME
    echo $TABLENAME   
    echo "bq load --source_format=CSV --skip_leading_rows=1 --autodetect=true hubspot_copy.${TABLENAME} ./${FILE_NAME}"    
    bq load --source_format=CSV --skip_leading_rows=1 --autodetect=true hubspot_copy.$TABLENAME ./$FILE_NAME
done
```