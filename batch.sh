# script d'import des données DFI de la DGFiP dans postgresql

psql -c "
-- table pour l'import des données brutes
CREATE TABLE IF NOT EXISTS dgfip_dfi_raw (raw text);
TRUNCATE dgfip_dfi_raw;
"

# import fichier texte dans fichier 'raw' intermédiaire
for f in *.txt.zip
do
  txt=$(echo $f | sed 's/.zip//')
  7z e $f
  psql -c "\copy dgfip_dfi_raw FROM '$txt' WITH (FORMAT CSV, HEADER FALSE, DELIMITER '!')"
  rm $txt
done

psql -c "

-- table de description des DFI
DROP TABLE IF EXISTS dgfip_dfi;
CREATE TABLE dgfip_dfi (depcom char(5), id_dfi char(7), nature_dfi char(1), validation char(8), lot char(5), meres text[], filles text[]);

-- création des DFI et parcelles 'mères'
INSERT INTO dgfip_dfi SELECT left(raw,2)||substr(raw,5,3) as depcom, substr(raw,13,7) as id_dfi, substr(raw,21,1) as nature_dfi, substr(raw,23,8) as validation, substr(raw,63,5) as lot, string_to_array(substr(replace(replace(left(substr(raw,70),-1),' ','0'),';',';'||left(raw,2)||substr(raw,5,3)||substr(raw,9,3)),2),';') as meres FROM dgfip_dfi_raw WHERE substr(raw,69,1)='1';

-- index sur code commune, dfi et lot (en principe unique)
CREATE INDEX dgfip_dfi_unique ON dgfip_dfi (depcom,id_dfi,lot);

-- ajout des parcelles 'mères'
WITH u AS (SELECT left(raw,2)||substr(raw,5,3) as u_depcom, substr(raw,13,7) as u_id_dfi, substr(raw,63,5) as u_lot, string_to_array(substr(replace(replace(left(substr(raw,70),-1),' ','0'),';',';'||left(raw,2)||substr(raw,5,3)||substr(raw,9,3)),2),';') as parcelles FROM dgfip_dfi_raw WHERE substr(raw,69,1)='2') UPDATE dgfip_dfi SET filles=parcelles from u WHERE depcom=u_depcom AND id_dfi=u_id_dfi and lot = u_lot;

-- index sur les parcelles mères et filles
CREATE INDEX dgfip_dfi_meres ON dgfip_dfi USING GIN (meres);
CREATE INDEX dgfip_dfi_filles ON dgfip_dfi USING GIN (filles);

-- suppression de la table 'raw' intermédiaire
-- DROP TABLE dgfip_dfi_raw;
"

# export JSON
psql -c "\copy (select json_build_object('depcom',depcom, 'id_dfi',id_dfi, 'lot_dfi',lot, 'nature',nature_dfi, 'date',validation::date, 'meres',meres, 'filles',filles) from dgfip_dfi) to dfi.json;"

# export postgresql
pg_dump -t dgfip_dfi -o -O -x > dfi.sql

# compression gzip
gzip -9 dfi.sql
gzip -9 dfi.json

