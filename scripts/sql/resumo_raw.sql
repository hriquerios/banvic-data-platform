-- Volume e tipagem de cada tabela do schema raw (descoberta dinâmica)
SELECT t.table_name AS tabela,
       (xpath('/row/n/text()',
              query_to_xml(format('SELECT COUNT(*) AS n FROM raw.%I', t.table_name), false, true, '')))[1]::text::bigint AS registros,
       COUNT(c.column_name) FILTER (WHERE c.column_name NOT LIKE '\_sdc%') AS colunas_negocio,
       string_agg(DISTINCT c.data_type, ', ') FILTER (WHERE c.column_name NOT LIKE '\_sdc%') AS tipos_encontrados
FROM information_schema.tables t
JOIN information_schema.columns c USING (table_schema, table_name)
WHERE t.table_schema = 'raw'
GROUP BY t.table_name
ORDER BY registros DESC;
